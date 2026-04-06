# 将仓库根目录 .env 载入当前 PowerShell 进程环境（供 dev.ps1 启动的 API 子窗口使用）。
# 不打印密钥；与 python-dotenv 常见格式兼容：KEY=VALUE、可选引号、# 注释、空行跳过。

function Import-RepoDotEnv {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RepoRoot
    )

    $path = Join-Path $RepoRoot ".env"
    if (-not (Test-Path -LiteralPath $path)) {
        return
    }

    Get-Content -LiteralPath $path -Encoding utf8 | ForEach-Object {
        $line = $_.Trim()
        if ($line.Length -eq 0 -or $line.StartsWith("#")) {
            return
        }
        $eq = $line.IndexOf("=")
        if ($eq -le 0) {
            return
        }
        $name = $line.Substring(0, $eq).Trim().TrimStart([char]0xFEFF)
        if ($name.Length -eq 0) {
            return
        }
        $val = $line.Substring($eq + 1).Trim()
        $len = $val.Length
        if (
            ($len -ge 2 -and $val.StartsWith('"') -and $val.EndsWith('"')) -or
            ($len -ge 2 -and $val.StartsWith("'") -and $val.EndsWith("'"))
        ) {
            $val = $val.Substring(1, $len - 2)
        }
        Set-Item -LiteralPath ("Env:" + $name) -Value $val
    }
}
