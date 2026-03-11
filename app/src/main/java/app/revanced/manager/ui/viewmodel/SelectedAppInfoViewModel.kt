package app.revanced.manager.ui.viewmodel

import android.content.pm.PackageInfo
import android.os.Parcelable
import androidx.annotation.StringRes
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.lifecycle.viewmodel.compose.SavedStateHandleSaveableApi
import androidx.lifecycle.viewmodel.compose.saveable
import app.revanced.manager.R
import app.revanced.manager.domain.manager.PreferencesManager
import app.revanced.manager.domain.repository.DownloadedAppRepository
import app.revanced.manager.domain.repository.DownloaderRepository
import app.revanced.manager.domain.repository.InstalledAppRepository
import app.revanced.manager.domain.repository.PatchBundleRepository
import app.revanced.manager.domain.repository.PatchOptionsRepository
import app.revanced.manager.domain.repository.PatchSelectionRepository
import app.revanced.manager.patcher.patch.PatchBundleInfo
import app.revanced.manager.patcher.patch.PatchBundleInfo.Extensions.requiredOptionsSet
import app.revanced.manager.patcher.patch.PatchBundleInfo.Extensions.toPatchSelection
import app.revanced.manager.ui.model.SelectedSource
import app.revanced.manager.ui.model.SelectedVersion
import app.revanced.manager.ui.model.navigation.Patcher
import app.revanced.manager.ui.model.navigation.SelectedAppInfo
import app.revanced.manager.util.Options
import app.revanced.manager.util.PM
import app.revanced.manager.util.PatchSelection
import app.revanced.manager.util.patchCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.parcelize.Parcelize
import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import java.io.File

@OptIn(SavedStateHandleSaveableApi::class)
class SelectedAppInfoViewModel(
    private val input: SelectedAppInfo.ViewModelParams
) : ViewModel(), KoinComponent {
    private val bundleRepository: PatchBundleRepository = get()
    private val selectionRepository: PatchSelectionRepository = get()
    private val optionsRepository: PatchOptionsRepository = get()
    private val downloaderRepository: DownloaderRepository = get()
    private val installedAppRepository: InstalledAppRepository = get()
    private val downloadedAppRepository: DownloadedAppRepository = get()
    private val pm: PM = get()
    private val savedStateHandle: SavedStateHandle = get()
    val prefs: PreferencesManager = get()

    val downloaders = downloaderRepository.loadedDownloadersFlow
    val packageName = input.packageName
    val localPath = input.localPath
    private val persistConfiguration = input.patches == null

    private val selectionFlow = MutableStateFlow(
        input.patches?.let(SelectionState::Customized) ?: SelectionState.Default
    )

    private val _selectedVersion = MutableStateFlow<SelectedVersion>(SelectedVersion.Auto)
    val selectedVersion: StateFlow<SelectedVersion> = _selectedVersion

    private val _selectedSource = MutableStateFlow<SelectedSource>(SelectedSource.Auto)
    val selectedSource: StateFlow<SelectedSource> = _selectedSource

    val bundles = bundleRepository.scopedBundleInfoFlow(packageName, null)

    val patchSelection = combine(selectionFlow, bundles) { selection, bundleInfo ->
        selection.patches(bundleInfo, allowIncompatible = true)
    }

    val customSelection = combine(selectionFlow, bundles) { selection, bundleInfo ->
        (selection as? SelectionState.Customized)?.patches(bundleInfo, allowIncompatible = true)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    val mostCompatibleVersions = patchSelection.flatMapLatest { selection ->
        bundleRepository.suggestedVersions(packageName, selection)
    }

    val resolvedVersion = combine(
        _selectedVersion,
        mostCompatibleVersions,
    ) { selected, mostCompatible ->
        when (selected) {
            is SelectedVersion.Specific -> selected.version
            is SelectedVersion.Any -> null
            is SelectedVersion.Auto -> mostCompatible?.maxWithOrNull(
                compareBy<Map.Entry<String, Int>> { it.value }
                    .thenBy { it.key }
            )?.key
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    val scopedBundles = resolvedVersion.flatMapLatest { version ->
        bundleRepository.scopedBundleInfoFlow(packageName, version)
    }

    val incompatiblePatchCount = scopedBundles.map { scoped ->
        scoped.sumOf { it.incompatible.size }
    }

    val resolvedSource = combine(
        _selectedSource,
        resolvedVersion,
    ) { source, version ->
        when (source) {
            is SelectedSource.Installed -> source
            is SelectedSource.Local -> source
            is SelectedSource.Downloaded -> source
            is SelectedSource.Plugin -> source
            is SelectedSource.Auto -> {
                val installedPackage = pm.getPackageInfo(packageName)
                val isPatched = installedAppRepository.get(packageName) != null
                val isSplit = installedPackage?.applicationInfo?.splitSourceDirs?.isEmpty() == false

                if (installedPackage != null && !isPatched && !isSplit && (version == null || installedPackage.versionName == version)) {
                    SelectedSource.Installed
                } else {
                    val app = version?.let {
                        downloadedAppRepository.get(packageName, it)
                    }
                    val file = app?.let {
                        downloadedAppRepository.getApkFileForApp(it)
                    }
                    file?.let { SelectedSource.Downloaded(it.path, version) }
                        ?: SelectedSource.Plugin(null)
                }
            }
        }
    }

    val bundleInfoFlow = bundleRepository.scopedBundleInfoFlow(packageName, null)

    var options: Options by savedStateHandle.saveable {
        viewModelScope.launch {
            if (!persistConfiguration) return@launch

            val bundlePatches = bundleInfoFlow.first()
                .associate { bundle ->
                    bundle.uid to bundle.patches.associateBy { patch -> patch.name }
                }

            options = withContext(Dispatchers.Default) {
                optionsRepository.getOptions(packageName, bundlePatches)
            }
        }

        mutableStateOf(emptyMap())
    }
        private set

    val errorFlow = combine(downloaders, resolvedSource) { downloaderList, source ->
        when {
            source is SelectedSource.Plugin && downloaderList.isEmpty() -> Error.NoDownloaders
            else -> null
        }
    }

    var selectedAppInfo: PackageInfo? by mutableStateOf(null)
        private set

    fun updateVersion(version: SelectedVersion) {
        _selectedVersion.value = version
    }

    fun updateSource(source: SelectedSource) {
        _selectedSource.value = source
    }

    fun updateConfiguration(
        selection: PatchSelection?,
        selectedOptions: Options
    ) = viewModelScope.launch {
        selectionFlow.value = selection?.let(SelectionState::Customized) ?: SelectionState.Default

        val filteredOptions = selectedOptions.filtered(bundleInfoFlow.first())
        options = filteredOptions

        if (persistConfiguration) {
            selection?.let { selectionRepository.updateSelection(packageName, it) }
                ?: selectionRepository.resetSelectionForPackage(packageName)

            optionsRepository.saveOptions(packageName, filteredOptions)
        }
    }

    private fun invalidateSelectedAppInfo() = viewModelScope.launch {
        selectedAppInfo = pm.getPackageInfo(packageName)
    }

    fun getOptionsFiltered(bundles: List<PatchBundleInfo.Scoped>) = options.filtered(bundles)

    suspend fun hasSetRequiredOptions(patchSelection: PatchSelection) = bundleInfoFlow
        .first()
        .requiredOptionsSet(
            allowIncompatible = prefs.disablePatchVersionCompatCheck.get(),
            isSelected = { bundle, patch -> patch.name in patchSelection[bundle.uid]!! },
            optionsForPatch = { bundle, patch -> options[bundle.uid]?.get(patch.name) },
        )

    suspend fun getPatcherParams(): Patcher.ViewModelParams {
        val bundles = bundleInfoFlow.first()
        return Patcher.ViewModelParams(
            input.packageName,
            resolvedVersion.first(),
            resolvedSource.first(),
            patchSelection.first(),
            getOptionsFiltered(bundles)
        )
    }

    init {
        invalidateSelectedAppInfo()

        input.localPath?.let { local ->
            viewModelScope.launch {
                val packageInfo = pm.getPackageInfo(File(local))

                _selectedVersion.value = SelectedVersion.Specific(
                    packageInfo?.versionName ?: return@launch
                )
                _selectedSource.value = SelectedSource.Local(local)
            }
        }

        viewModelScope.launch {
            if (prefs.disableSelectionWarning.get()) {
                val previous = selectionRepository.getSelection(packageName)
                if (previous.patchCount == 0) return@launch
                selectionFlow.value = SelectionState.Customized(previous)
            }
        }
    }

    enum class Error(@param:StringRes val resourceId: Int) {
        NoDownloaders(R.string.no_downloader_available)
    }

    private companion object {
        private fun Options.filtered(bundles: List<PatchBundleInfo.Scoped>): Options =
            buildMap options@{
                bundles.forEach bundles@{ bundle ->
                    val bundleOptions = this@filtered[bundle.uid] ?: return@bundles
                    val patches = bundle.patches.associateBy { it.name }

                    this@options[bundle.uid] = buildMap bundleOptions@{
                        bundleOptions.forEach patch@{ (patchName, values) ->
                            val validOptionKeys =
                                patches[patchName]?.options?.map { it.name }?.toSet() ?: return@patch

                            this@bundleOptions[patchName] = values.filterKeys { key ->
                                key in validOptionKeys
                            }
                        }
                    }
                }
            }
    }
}

private sealed interface SelectionState : Parcelable {
    fun patches(bundles: List<PatchBundleInfo.Scoped>, allowIncompatible: Boolean): PatchSelection

    @Parcelize
    data class Customized(val patchSelection: PatchSelection) : SelectionState {
        override fun patches(bundles: List<PatchBundleInfo.Scoped>, allowIncompatible: Boolean) =
            bundles.toPatchSelection(
                allowIncompatible
            ) { uid, patch ->
                patchSelection[uid]?.contains(patch.name) ?: false
            }
    }

    @Parcelize
    data object Default : SelectionState {
        override fun patches(bundles: List<PatchBundleInfo.Scoped>, allowIncompatible: Boolean) =
            bundles.toPatchSelection(allowIncompatible) { _, patch -> patch.include }
    }
}
