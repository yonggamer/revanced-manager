package app.revanced.manager.ui.viewmodel

import android.content.pm.PackageInfo
import android.util.Log
import androidx.compose.runtime.derivedStateOf
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import app.revanced.manager.data.room.apps.installed.InstalledApp
import app.revanced.manager.domain.installer.RootInstaller
import app.revanced.manager.domain.manager.PreferencesManager
import app.revanced.manager.domain.repository.DownloadedAppRepository
import app.revanced.manager.domain.repository.InstalledAppRepository
import app.revanced.manager.domain.repository.PatchBundleRepository
import app.revanced.manager.network.downloader.APKMirror
import app.revanced.manager.network.downloader.AppDownloader
import app.revanced.manager.ui.model.SelectedApp
import app.revanced.manager.util.PM
import app.revanced.manager.util.mutableStateSetOf
import app.revanced.manager.util.simpleMessage
import app.revanced.manager.util.tag
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject

class VersionSelectorViewModel(
    val packageName: String
) : ViewModel(), KoinComponent {
    private val downloadedAppRepository: DownloadedAppRepository by inject()
    private val installedAppRepository: InstalledAppRepository by inject()
    private val patchBundleRepository: PatchBundleRepository by inject()
    private val pm: PM by inject()
    private val prefs: PreferencesManager by inject()
    private val appDownloader: AppDownloader = APKMirror()
    val rootInstaller: RootInstaller by inject()

    var installedApp: Pair<PackageInfo, InstalledApp?>? by mutableStateOf(null)
        private set
    var isLoading by mutableStateOf(true)
        private set
    var errorMessage: String? by mutableStateOf(null)
        private set

    var forcedVersion: String? by mutableStateOf(null)
        private set

    // TODO: this is a bad name
    var nonSuggestedVersion by mutableStateOf<SelectedApp.Installed?>(null)
        private set
    val showNonSuggestedVersionDialog by derivedStateOf { nonSuggestedVersion != null }

    private val forcedVersionAsync = viewModelScope.async(Dispatchers.Default) {
        if (!prefs.suggestedVersionSafeguard.get()) return@async null

        patchBundleRepository.suggestedVersions.first()[packageName]
    }.also {
        viewModelScope.launch {
            forcedVersion = it.await()
        }
    }

    val downloadableVersions = mutableStateSetOf<SelectedApp.Download>()

    val supportedVersions = patchBundleRepository.bundles.map { bundles ->
        // It is mandatory to use the suggested version if the safeguard is enabled.
        forcedVersionAsync.await()?.let { version ->
            return@map mapOf(
                version to bundles
                    .asSequence()
                    .flatMap { (_, bundle) -> bundle.patches }
                    .flatMap { it.compatiblePackages.orEmpty() }
                    .filter { it.packageName == packageName }
                    .count { it.versions.isNullOrEmpty() || version in it.versions }
            )
        }

        var patchesWithoutVersions = 0

        bundles.flatMap { (_, bundle) ->
            bundle.patches.flatMap { patch ->
                patch.compatiblePackages.orEmpty()
                    .filter { it.packageName == packageName }
                    .onEach { if (it.versions == null) patchesWithoutVersions++ }
                    .flatMap { it.versions.orEmpty() }
            }
        }.groupingBy { it }
            .eachCount()
            .toMutableMap()
            .apply {
                replaceAll { _, count ->
                    count + patchesWithoutVersions
                }
            }
    }.flowOn(Dispatchers.Default)

    val downloadedVersions = downloadedAppRepository.getAll().map { downloadedApps ->
        downloadedApps.filter { it.packageName == packageName }.map {
            SelectedApp.Local(
                it.packageName,
                it.version,
                downloadedAppRepository.getApkFileForApp(it),
                false
            )
        }
    }

    init {
        viewModelScope.launch(Dispatchers.Main) {
            val packageInfo = async(Dispatchers.IO) { pm.getPackageInfo(packageName) }
            val installedAppDeferred =
                async(Dispatchers.IO) { installedAppRepository.get(packageName) }

            installedApp =
                packageInfo.await()?.let {
                    it to installedAppDeferred.await()
                }
        }

        viewModelScope.launch(Dispatchers.IO) {
            try {
                val compatibleVersions = supportedVersions.first()

                appDownloader.getAvailableVersions(
                    packageName,
                    compatibleVersions.keys
                ).collect {
                    if (it.version in compatibleVersions || compatibleVersions.isEmpty()) {
                        downloadableVersions.add(
                            SelectedApp.Download(
                                packageName,
                                it.version,
                                it
                            )
                        )
                    }
                }

                withContext(Dispatchers.Main) {
                    isLoading = false
                }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) {
                    Log.e(tag, "Failed to load apps", e)
                    errorMessage = e.simpleMessage()
                }
            }
        }
    }

    fun dismissNonSuggestedVersionDialog() {
        nonSuggestedVersion = null
    }

    fun continueWithNonSuggestedVersion(dismissPermanently: Boolean) = viewModelScope.launch {
        if (dismissPermanently) prefs.suggestedVersionSafeguard.update(false)
        dismissNonSuggestedVersionDialog()
    }

    fun isAppVersionAllowed(app: SelectedApp.Installed): Boolean {
        if (forcedVersion == null) return true

        return (app.version == forcedVersion).also { result ->
            if (!result) nonSuggestedVersion = app
        }
    }
}