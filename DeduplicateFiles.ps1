# DeduplicateFiles.ps1 version 1.0.0

param(
    [string[]]$SourceRoots,
    [string]$LibraryRoot,
    [string[]]$FilePatterns = @("*.jpg","*.jpeg","*.png","*.gif","*.tif","*.tiff","*.bmp","*.mp4","*.mov","*.avi","*.mkv"),
    [switch]$DryRun,
    [switch]$Help,
    [Alias("Verbose")][switch]$VerboseOutput,
    [switch]$Silent,
    [switch]$LinkOnlyDuplicates,
    [switch]$Cleanup,
    [Alias("v")][switch]$Version,
    [string]$LogPath
)

$script:ScriptVersion = "1.0.0"

# ----------------------------------------------
# HELP
# ----------------------------------------------
function Show-Help {
@"
Deduplicate-Files.ps1
---------------------

Deduplicates files across one or more folders by scanning for identical
content (SHA-256 hash). Unique originals are moved into a master library:

    <LibraryRoot>\<hash-prefix>\<full-hash>\<original-name>

All original paths are replaced by NTFS hard links, so filenames and
folder structure in your project folders stay the same.

USAGE:
DeduplicateFiles.ps1 -SourceRoots <paths> -LibraryRoot <path> [-Verbose] [-DryRun] [-Cleanup]

EXAMPLE (dry-run):
  DeduplicateFiles.ps1 `
      -SourceRoots "C:\Users\Me\Pictures" `
      -LibraryRoot "C:\Users\Me\Desktop\DeDuped" `
      -DryRun

PARAMETERS:
  -SourceRoots   One or more root folders to scan for files.
  -LibraryRoot   Master folder where unique originals will be stored.
                 Will be created if it does not exist.
  -FilePatterns  File patterns to treat as files (default: common image/video formats).
  -Silent        Suppress console output (logging still written). -Verbose wins if both set.
  -Cleanup       Delete library files that are the last remaining hard link (after prompt).
  -Verbose       Show per-file actions.
  -DryRun        Simulate only. No files are moved, deleted or linked.
  -LogPath       Optional explicit log file path.
  -Help          Show this help.

NOTES:
  - All folders must be on the same drive (hard link limitation).
  - Duplicates are detected by SHA-256 hash of file contents.
    File names and paths do not matter for uniqueness.
  - Progress shows hashing stats, deduped count, and space saved.
  - Script waits for a key press before closing when finished.
"@
}
# Advanced option (not shown in help): -LinkOnlyDuplicates keeps the first occurrence in place,
# creates a library hard-link alias to it, and only rewrites subsequent duplicates as links.
# Use only if you understand the maintenance trade-offs.


if ($Version) {
    Write-Host "DeduplicateFiles.ps1 version $($script:ScriptVersion)"
    exit 0
}

if ($Help -or -not $LibraryRoot -or (-not $SourceRoots -and -not $Cleanup)) {
    Show-Help
    exit 0
}

# ----------------------------------------------
# PREP & LOGGING
# ----------------------------------------------

# Always ensure library folder exists (even in dry-run so logging works)
if (-not (Test-Path $LibraryRoot)) {
    Write-Host "[INFO] Creating library root folder: $LibraryRoot"
    New-Item -ItemType Directory -Path $LibraryRoot | Out-Null
}

# Default log if none provided
if (-not $LogPath) {
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $LogPath = Join-Path $LibraryRoot "Deduplicate-Files_$timestamp.log"
}

# Start log
"Deduplicate run started at $(Get-Date)" | Out-File $LogPath
"DryRun = $DryRun" | Add-Content $LogPath
"Verbose = $VerboseOutput" | Add-Content $LogPath
"Silent = $Silent" | Add-Content $LogPath
"LinkOnlyDuplicates = $LinkOnlyDuplicates" | Add-Content $LogPath
"Cleanup = $Cleanup" | Add-Content $LogPath
"LibraryRoot = $LibraryRoot" | Add-Content $LogPath
"SourceRoots = $($SourceRoots -join ', ')" | Add-Content $LogPath
"" | Add-Content $LogPath

function Write-Log([string]$msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') `t $msg"
    if (-not ($Silent -and -not $VerboseOutput)) {
        Write-Host $msg
    }
    Add-Content $LogPath $line
}

function Write-Detail([string]$msg) {
    if (-not $VerboseOutput) { return }
    Write-Log $msg
}

function Replace-WithHardLink {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Target
    )

    if ($DryRun) {
        Write-Detail "[DRYRUN] Would replace $Path with hard link -> $Target"
        return $true
    }

    $dir = Split-Path -Parent $Path
    $leaf = Split-Path -Leaf $Path
    $backup = Join-Path $dir ($leaf + ".bak_" + [guid]::NewGuid().ToString("N"))

    try {
        Move-Item -LiteralPath $Path -Destination $backup -ErrorAction Stop
    } catch {
        Write-Log "[ERROR] Failed to move $Path to backup ($backup): $($_.Exception.Message)"
        return $false
    }

    $linked = $false
    try {
        New-Item -ItemType HardLink -Path $Path -Target $Target -ErrorAction Stop | Out-Null
        $linked = $true
    } catch {
        Write-Log "[ERROR] Failed to create hard link $Path -> $($Target): $($_.Exception.Message). Restoring original."
    }

    if ($linked) {
        Write-Detail "Replaced $Path with hard link -> $Target"
        try {
            Remove-Item -LiteralPath $backup -Force -ErrorAction Stop
        } catch {
            Write-Log "[WARN] Linked successfully but could not delete backup $($backup): $($_.Exception.Message)"
        }
    } else {
        try {
            Move-Item -LiteralPath $backup -Destination $Path -ErrorAction Stop
        } catch {
            Write-Log "[ERROR] Failed to restore original $Path from $($backup): $($_.Exception.Message)"
        }
    }

    return $linked
}

function Ensure-LibraryAlias {
    param(
        [Parameter(Mandatory = $true)][string]$AliasPath,
        [Parameter(Mandatory = $true)][string]$TargetPath
    )

    if ($AliasPath -eq $TargetPath) { return }
    if (Test-Path -LiteralPath $AliasPath) { return }

    if ($DryRun) {
        Write-Detail "[DRYRUN] Would add library alias $AliasPath -> $TargetPath"
        return
    }

    try {
        New-Item -ItemType HardLink -Path $AliasPath -Target $TargetPath -ErrorAction Stop | Out-Null
        Write-Detail "Added library alias $AliasPath -> $TargetPath"
    } catch {
        Write-Log "[ERROR] Could not add library alias $AliasPath -> $($TargetPath): $($_.Exception.Message)"
    }
}

function Format-Bytes([long]$bytes) {
    $units = @("B","KB","MB","GB","TB","PB")
    $size = [double]$bytes
    $i = 0
    while ($size -ge 1024 -and $i -lt $units.Length - 1) {
        $size = $size / 1024
        $i++
    }
    return ("{0:N2} {1}" -f $size, $units[$i])
}

$script:Win32Loaded = $false
function Get-LinkCount {
    param([string]$Path)

    if (-not $script:Win32Loaded) {
        $typeDef = @"
using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

public static class NativeMethods {
    [StructLayout(LayoutKind.Sequential)]
    public struct FILETIME {
        public uint dwLowDateTime;
        public uint dwHighDateTime;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BY_HANDLE_FILE_INFORMATION {
        public uint FileAttributes;
        public FILETIME CreationTime;
        public FILETIME LastAccessTime;
        public FILETIME LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern bool GetFileInformationByHandle(SafeFileHandle hFile, out BY_HANDLE_FILE_INFORMATION lpFileInformation);
}
"@
        Add-Type -Language CSharp -TypeDefinition $typeDef -ErrorAction Stop | Out-Null
        $script:Win32Loaded = $true
    }

    try {
        $fs = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    } catch {
        return $null
    }

    $info = New-Object NativeMethods+BY_HANDLE_FILE_INFORMATION
    $ok = [NativeMethods]::GetFileInformationByHandle($fs.SafeFileHandle, [ref]$info)
    $fs.Dispose()

    if (-not $ok) { return $null }
    return [int]$info.NumberOfLinks
}

# ----------------------------------------------
# CLEANUP MODE (remove library-only files)
# ----------------------------------------------

if ($Cleanup) {
    Write-Log "[CLEANUP] Scanning library for files with only one hard link (library-only)."

    $libFiles = Get-ChildItem -Path $LibraryRoot -Recurse -File -ErrorAction SilentlyContinue |
                Where-Object { $_.FullName -ne $LibraryRoot -and (Split-Path -Parent $_.FullName) -ne $LibraryRoot }
    $candidates = @()
    $scanIndex = 0
    $scanTotal = $libFiles.Count

    foreach ($lf in $libFiles) {
        $scanIndex++
        $scanPercent = [int](($scanIndex / [math]::Max($scanTotal,1)) * 100)
        $scanStatus = "Scanning {0}/{1}" -f $scanIndex, $scanTotal
        Write-Progress -Id 10 -Activity "Cleanup scan" -Status $scanStatus -PercentComplete $scanPercent

        $linkCount = Get-LinkCount -Path $lf.FullName
        if (-not $linkCount) {
            Write-Log "[WARN] Could not read link count for $($lf.FullName)"
            continue
        }

        if ($linkCount -eq 1) {
            $candidates += [pscustomobject]@{ Path = $lf.FullName; Length = $lf.Length }
        }
    }

    Write-Progress -Id 10 -Activity "Cleanup scan" -Completed

    if (-not $candidates -or $candidates.Count -eq 0) {
        Write-Log "[CLEANUP] No library-only files found. Nothing to delete."
        exit 0
    }

    $totalSize = ($candidates | Measure-Object Length -Sum).Sum
    $summary = "[CLEANUP] Found {0} file(s) totaling {1}. These appear to have no other hard links (library-only)." -f $candidates.Count, (Format-Bytes $totalSize)
    Write-Host $summary
    Write-Host "WARNING: Deleting will remove the last copies of these files (including backups)."

    function Prompt-Cleanup {
        param([string]$PromptText)
        while ($true) {
            $resp = Read-Host $PromptText
            switch ($resp.ToUpperInvariant()) {
                "Y" { return "Delete" }
                "L" { return "List" }
                "C" { return "Cancel" }
                default { Write-Host "Enter Y (delete), L (list), or C (cancel)." }
            }
        }
    }

    while ($true) {
        $choice = Prompt-Cleanup -PromptText "Proceed? [Y=delete, L=list, C=cancel]"
        if ($choice -eq "Cancel") {
            Write-Log "[CLEANUP] Cancelled."
            exit 0
        }

        if ($choice -eq "List") {
            Write-Host "Files to delete:"
            $candidates | ForEach-Object { Write-Host ("  {0} ({1})" -f $_.Path, (Format-Bytes $_.Length)) }
            Write-Host ""
            continue
        }

        # Delete
        $delIndex = 0
        $deleted = 0
        $deletedBytes = [int64]0
        foreach ($item in $candidates) {
            $delIndex++
            $delPercent = [int](($delIndex / [math]::Max($candidates.Count,1)) * 100)
            $delStatus = "Deleting {0}/{1}" -f $delIndex, $candidates.Count
            Write-Progress -Id 11 -Activity "Cleanup delete" -Status $delStatus -PercentComplete $delPercent

            if ($DryRun) {
                Write-Log "[DRYRUN][CLEANUP] Would delete $($item.Path)"
                $deleted++
                $deletedBytes += $item.Length
                continue
            }

            try {
                Remove-Item -LiteralPath $item.Path -Force -ErrorAction Stop
                $deleted++
                $deletedBytes += $item.Length
                Write-Detail "[CLEANUP] Deleted $($item.Path)"

                # Clean empty parent folders up to the library root (hash folders)
                $parent = Split-Path -Parent $item.Path
                for ($i = 0; $i -lt 3; $i++) {
                    if ($parent -and ($parent -like "$LibraryRoot*") -and ($parent -ne $LibraryRoot) -and (Get-ChildItem -LiteralPath $parent -Force | Measure-Object).Count -eq 0) {
                        Remove-Item -LiteralPath $parent -Force
                        Write-Detail "[CLEANUP] Removed empty folder $parent"
                        $parent = Split-Path -Parent $parent
                    } else {
                        break
                    }
                }
            } catch {
                Write-Log "[ERROR][CLEANUP] Failed to delete $($item.Path): $($_.Exception.Message)"
            }
        }

        Write-Progress -Id 11 -Activity "Cleanup delete" -Completed

        Write-Log "[CLEANUP] Deleted $deleted file(s); space freed (logical size): $(Format-Bytes $deletedBytes)."
        if ($DryRun) { Write-Log "[CLEANUP] Dry-run only - no deletions performed." }
        Write-Host "Press any key to close..."
        [void][System.Console]::ReadKey($true)
        exit 0
    }
}

# ----------------------------------------------
# VALIDATION
# ----------------------------------------------

$libItem = Get-Item $LibraryRoot
$libDrive = $libItem.PSDrive.Name
$normalizedLibrary = $libItem.FullName.ToLowerInvariant()

foreach ($root in $SourceRoots) {
    if (-not (Test-Path $root)) {
        Write-Log "[WARN] Source root missing: $root - skipping"
        continue
    }

    $rootDrive = (Get-Item $root).PSDrive.Name
    if ($rootDrive -ne $libDrive) {
        throw "ERROR: $root is on drive $rootDrive but library is on $libDrive. Hard links cannot cross drives."
    }
}

# ----------------------------------------------
# SCAN FOR FILES (with progress)
# ----------------------------------------------

$extensionSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
foreach ($pattern in $FilePatterns) {
    $clean = $pattern -replace '^\*\.', ''
    if (-not [string]::IsNullOrWhiteSpace($clean)) {
        $extensionSet.Add($clean) | Out-Null
    }
}

$allFiles = New-Object System.Collections.Generic.List[System.IO.FileInfo]
$scanCount = 0
$rootTotal = $SourceRoots.Count
$rootIndex = 0

foreach ($root in $SourceRoots) {
    $rootIndex++
    $percent = [int](($rootIndex / [math]::Max($rootTotal,1)) * 100)
    Write-Progress -Id 1 -Activity "Scanning for files" -Status "Root ${rootIndex}/${rootTotal}: $root" -PercentComplete $percent

    Get-ChildItem -Path $root -Recurse -File -ErrorAction SilentlyContinue |
        ForEach-Object {
            $ext = [System.IO.Path]::GetExtension($_.Name)
            if ($ext.StartsWith('.')) { $ext = $ext.Substring(1) }

            if ($extensionSet.Contains($ext) -and $_.FullName.ToLowerInvariant() -notlike "$normalizedLibrary*") {
                $allFiles.Add($_) | Out-Null
                $scanCount++
                if (($scanCount % 500) -eq 0) {
                    Write-Progress -Id 2 -ParentId 1 -Activity "Collecting files" -Status "$scanCount files found..." -PercentComplete 0
                }
            }
        }
}

Write-Progress -Id 2 -Activity "Collecting files" -Completed
Write-Progress -Id 1 -Activity "Scanning for files" -Completed

$allFiles = $allFiles | Sort-Object -Property FullName -Unique
$totalFiles = $allFiles.Count

if ($totalFiles -eq 0) {
    Write-Log "No files found to process."
    exit 0
}

Write-Log "Found $totalFiles files."

# ----------------------------------------------
# DEDUPLICATION ENGINE
# ----------------------------------------------

$hashToMaster = @{}
$index = 0
$dedupedCount = 0
$spaceSavedBytes = [int64]0

foreach ($file in $allFiles) {
    $index++
    $status = "{0}/{1} | deduped {2} | saved {3}" -f $index, $totalFiles, $dedupedCount, (Format-Bytes $spaceSavedBytes)
    Write-Progress -Activity "Hashing and comparing" -Status $status -PercentComplete (($index / $totalFiles) * 100)

    # Compute hash
    try {
        $hash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
    } catch {
        Write-Log "[WARN] Failed to hash $($file.FullName) - skipping"
        continue
    }

    # Unique file?
    if (-not $hashToMaster.ContainsKey($hash)) {

        # Build library structure:
        #   <LibraryRoot>\<hash-prefix>\<full-hash>\<original-name>
        $prefix = $hash.Substring(0,2)
        $prefixFolder = Join-Path $LibraryRoot $prefix
        $hashFolder   = Join-Path $prefixFolder $hash
        $masterPath   = Join-Path $hashFolder $file.Name

        # Ensure folders exist
        foreach ($folder in @($prefixFolder, $hashFolder)) {
            if (-not (Test-Path $folder)) {
                if ($DryRun) {
                    Write-Detail "[DRYRUN] Would create folder: $folder"
                } else {
                    New-Item -ItemType Directory -Path $folder | Out-Null
                    Write-Detail "Created folder: $folder"
                }
            }
        }

        # If a master already exists in this hash folder (possibly different name), reuse it and add a name alias.
        $existingMaster = $null
        if (Test-Path $hashFolder) {
            $existingMaster = @(Get-ChildItem -LiteralPath $hashFolder -File -ErrorAction SilentlyContinue) | Select-Object -First 1
        }

        if ($existingMaster) {
            $masterPath = $existingMaster.FullName
            $hashToMaster[$hash] = $masterPath

            $aliasPath = Join-Path $hashFolder $file.Name
            Ensure-LibraryAlias -AliasPath $aliasPath -TargetPath $masterPath
            $didLink = Replace-WithHardLink -Path $file.FullName -Target $masterPath
            if ($didLink) {
                $dedupedCount++
                $spaceSavedBytes += $file.Length
                if ($LinkOnlyDuplicates -and -not $VerboseOutput) {
                    Write-Log "[DEDUP] $($file.FullName) -> $masterPath"
                }
            }
            continue
        }

        # Optional mode: keep first occurrence in place, only link duplicates to it (but still add a library alias).
        if ($LinkOnlyDuplicates) {
            $aliasPath = Join-Path $hashFolder $file.Name
            Ensure-LibraryAlias -AliasPath $aliasPath -TargetPath $file.FullName
            if (Test-Path -LiteralPath $aliasPath) {
                $canonicalMaster = $aliasPath
            } else {
                $canonicalMaster = $file.FullName
            }
            $hashToMaster[$hash] = $canonicalMaster
            Write-Detail "[INFO] Inline master for hash $($hash): $canonicalMaster"
            continue
        }

        if (-not (Test-Path $masterPath)) {
            # New master: move file into library, then link back. Restore on failure.
            if ($DryRun) {
                Write-Detail "[DRYRUN] Would move $($file.FullName) -> $masterPath"
                Write-Detail "[DRYRUN] Would create hardlink $($file.FullName) -> $masterPath"
                $hashToMaster[$hash] = $masterPath
                continue
            }

            try {
                Move-Item -LiteralPath $file.FullName -Destination $masterPath -ErrorAction Stop
                Write-Detail "Moved $($file.FullName) -> $masterPath"
            } catch {
                Write-Log "[ERROR] Failed to move $($file.FullName) -> $($masterPath): $($_.Exception.Message)"
                continue
            }

            try {
                New-Item -ItemType HardLink -Path $file.FullName -Target $masterPath -ErrorAction Stop | Out-Null
                Write-Detail "Linked $($file.FullName) -> $masterPath"
                $hashToMaster[$hash] = $masterPath
            } catch {
                Write-Log "[ERROR] Failed to create hard link $($file.FullName) -> $($masterPath): $($_.Exception.Message). Restoring original."
                try {
                    Move-Item -LiteralPath $masterPath -Destination $file.FullName -ErrorAction Stop
                    Write-Log "[INFO] Restored original file to $($file.FullName) after link failure"
                } catch {
                    Write-Log "[ERROR] Failed to restore original $($file.FullName) from $($masterPath): $($_.Exception.Message)"
                }
            }
        } else {
            Write-Detail "[INFO] Master already exists for hash at $masterPath"
            $hashToMaster[$hash] = $masterPath
            $didLink = Replace-WithHardLink -Path $file.FullName -Target $masterPath
            if ($didLink) {
                $dedupedCount++
                $spaceSavedBytes += $file.Length
            }
        }
    }
    else {
        # Duplicate file
        $masterPath = $hashToMaster[$hash]
        $hashFolder = Split-Path -Parent $masterPath
        $aliasPath = Join-Path $hashFolder $file.Name

        Ensure-LibraryAlias -AliasPath $aliasPath -TargetPath $masterPath
        $didLink = Replace-WithHardLink -Path $file.FullName -Target $masterPath
        if ($didLink) {
            $dedupedCount++
            $spaceSavedBytes += $file.Length
            if ($LinkOnlyDuplicates -and -not $VerboseOutput) {
                Write-Log "[DEDUP] $($file.FullName) -> $masterPath"
            }
        }
    }
}

Write-Log "DONE."
Write-Log "Deduped $dedupedCount file(s); space saved: $(Format-Bytes $spaceSavedBytes)."
if ($DryRun) { Write-Log "Dry-run only - no changes were made." }
Write-Host "Press any key to close..."
[void][System.Console]::ReadKey($true)
