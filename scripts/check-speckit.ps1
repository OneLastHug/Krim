[CmdletBinding()]
param([switch]$SmokeTest)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$oldInit = $env:SPECIFY_INIT_DIR
$oldFeature = $env:SPECIFY_FEATURE
$oldFeatureDirectory = $env:SPECIFY_FEATURE_DIRECTORY
$smokeRoot = $null

function Assert-Check {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
    Write-Host "[PASS] $Message"
}

function Invoke-BundledScript {
    param([string]$ProjectRoot, [string]$Name, [string[]]$Options = @())
    $scriptPath = Join-Path $ProjectRoot ('.specify/scripts/powershell/' + $Name)
    $start = New-Object System.Diagnostics.ProcessStartInfo
    $start.FileName = (Get-Command powershell.exe -ErrorAction Stop).Source
    $start.Arguments = '-NoProfile -File "' + $scriptPath + '" ' + ($Options -join ' ')
    $start.WorkingDirectory = $ProjectRoot
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $process = [System.Diagnostics.Process]::Start($start)
    try {
        $output = $process.StandardOutput.ReadToEnd()
        $errorOutput = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        return [PSCustomObject]@{
            ExitCode = $process.ExitCode
            Output = $output
            ErrorOutput = $errorOutput
        }
    }
    finally { $process.Dispose() }
}

function Read-ScriptJson {
    param([string]$ProjectRoot, [string]$Name, [string[]]$Options = @('-Json'))
    $result = Invoke-BundledScript $ProjectRoot $Name $Options
    if ($result.ExitCode -ne 0) {
        throw ("{0} exited {1}: {2}" -f $Name, $result.ExitCode, $result.ErrorOutput.Trim())
    }
    return ($result.Output | ConvertFrom-Json)
}

try {
    $env:SPECIFY_INIT_DIR = $root
    $integration = Get-Content -LiteralPath (Join-Path $root '.specify/integration.json') -Raw | ConvertFrom-Json
    Assert-Check ($integration.version -eq '1.3.2') 'Spec Kit version is 1.3.2'
    Assert-Check (($integration.installed_integrations -join ',') -eq 'claude') 'Only the Claude integration is installed'
    $options = $integration.integration_settings.claude.parsed_options
    Assert-Check ([string]$options.events -eq 'false') 'Native event handlers are disabled'
    Assert-Check ($options.no_model_invocation -eq $true) 'Skills require explicit invocation'
    Assert-Check (-not (Test-Path -LiteralPath (Join-Path $root '.specify/extensions/git'))) 'No Git extension is installed'

    $skills = @('analyze', 'checklist', 'clarify', 'constitution', 'converge', 'implement', 'plan', 'specify', 'tasks')
    foreach ($skill in $skills) {
        $name = 'speckit-' + $skill
        $text = Get-Content -LiteralPath (Join-Path $root ('.claude/skills/' + $name + '/SKILL.md')) -Raw
        Assert-Check (($text -match ('(?m)^name: "?' + [regex]::Escape($name) + '"?\r?$')) -and
            ($text -match '(?m)^user-invocable: true\r?$') -and
            ($text -match '(?m)^disable-model-invocation: true\r?$')) "Skill metadata: $name"
    }

    foreach ($name in @('claude', 'speckit')) {
        $manifest = Get-Content -LiteralPath (Join-Path $root ('.specify/integrations/' + $name + '.manifest.json')) -Raw | ConvertFrom-Json
        Assert-Check ($manifest.version -eq '1.3.2') "Manifest version: $name"
        $count = 0
        foreach ($file in $manifest.files.PSObject.Properties) {
            $path = Join-Path $root $file.Name
            $actual = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($actual -ne $file.Value) { throw "Official asset was modified: $($file.Name)" }
            $count++
        }
        Write-Host "[PASS] Official $name assets: $count SHA256 checks"
    }

    $scriptFiles = @(Get-ChildItem -LiteralPath (Join-Path $root '.specify/scripts/powershell') -Filter '*.ps1')
    foreach ($file in ($scriptFiles.FullName + @($PSCommandPath))) {
        $tokens = $null
        $parseErrors = $null
        [void][System.Management.Automation.Language.Parser]::ParseFile($file, [ref]$tokens, [ref]$parseErrors)
        if (@($parseErrors).Count -gt 0) { throw ($parseErrors | Out-String) }
    }
    Write-Host "[PASS] PowerShell syntax: $($scriptFiles.Count + 1) scripts"

    foreach ($name in @('spec', 'plan', 'tasks', 'constitution', 'checklist')) {
        $template = Read-ScriptJson $root 'resolve-template.ps1' @($name + '-template', '-Json')
        Assert-Check (-not [string]::IsNullOrWhiteSpace($template.TEMPLATE_CONTENT)) "Composed template: $name"
    }

    if ($SmokeTest) {
        $cache = Join-Path $root '.cache'
        [void](New-Item -ItemType Directory -Path $cache -Force)
        $smokeRoot = Join-Path $cache ('speckit-smoke-' + [guid]::NewGuid().ToString('N'))
        [void](New-Item -ItemType Directory -Path $smokeRoot)
        Copy-Item -LiteralPath (Join-Path $root '.specify') -Destination (Join-Path $smokeRoot '.specify') -Recurse
        $env:SPECIFY_INIT_DIR = $smokeRoot
        $env:SPECIFY_FEATURE = $null
        $env:SPECIFY_FEATURE_DIRECTORY = $null
        $feature = Join-Path $smokeRoot 'specs/001-smoke'
        [void](New-Item -ItemType Directory -Path $feature -Force)
        $utf8 = New-Object System.Text.UTF8Encoding($false)
        [IO.File]::WriteAllText((Join-Path $feature 'spec.md'), "# Smoke feature`n`nVerify the bundled workflow scripts.`n", $utf8)
        [IO.File]::WriteAllText((Join-Path $smokeRoot '.specify/feature.json'), '{"feature_directory":"specs/001-smoke"}', $utf8)

        $paths = Read-ScriptJson $smokeRoot 'check-prerequisites.ps1' @('-Json', '-PathsOnly')
        Assert-Check ($paths.FEATURE_DIR -eq $feature) 'Feature pointer resolves independently of the Git branch'
        $plan = Read-ScriptJson $smokeRoot 'setup-plan.ps1'
        Assert-Check ((Test-Path -LiteralPath $plan.IMPL_PLAN) -and ($plan.FEATURE_SPEC -eq (Join-Path $feature 'spec.md'))) 'Plan setup uses the selected feature'
        [IO.File]::WriteAllText($plan.IMPL_PLAN, "# Existing plan`n", $utf8)
        [void](Read-ScriptJson $smokeRoot 'setup-plan.ps1')
        Assert-Check ([IO.File]::ReadAllText($plan.IMPL_PLAN) -eq "# Existing plan`n") 'Plan setup preserves an existing plan'
        $tasks = Read-ScriptJson $smokeRoot 'setup-tasks.ps1'
        Assert-Check (-not [string]::IsNullOrWhiteSpace($tasks.TASKS_TEMPLATE_CONTENT)) 'Task setup resolves the official template'
        $tasksPath = Join-Path $feature 'tasks.md'
        [IO.File]::WriteAllText($tasksPath, "# Smoke tasks`n`n- [ ] T001 Verify fixture`n", $utf8)
        $ready = Read-ScriptJson $smokeRoot 'check-prerequisites.ps1' @('-Json', '-RequireSpec', '-RequireTasks', '-IncludeTasks')
        Assert-Check ($ready.AVAILABLE_DOCS -contains 'tasks.md') 'Implementation prerequisites recognize tasks.md'
        Remove-Item -LiteralPath $tasksPath
        $missing = Invoke-BundledScript $smokeRoot 'check-prerequisites.ps1' @('-Json', '-RequireTasks')
        Assert-Check (($missing.ExitCode -ne 0) -and ($missing.ErrorOutput -match 'tasks.md')) 'Implementation is blocked when tasks.md is missing'
    }
    Write-Host 'Spec Kit checks passed. No model or application service was invoked.'
}
finally {
    $env:SPECIFY_INIT_DIR = $oldInit
    $env:SPECIFY_FEATURE = $oldFeature
    $env:SPECIFY_FEATURE_DIRECTORY = $oldFeatureDirectory
    if ($smokeRoot -and (Test-Path -LiteralPath $smokeRoot)) {
        Remove-Item -LiteralPath $smokeRoot -Recurse -Force
    }
}
