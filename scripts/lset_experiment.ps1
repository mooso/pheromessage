$time = 60
For ($nodes = 1000; $nodes -le 25000; $nodes += 2000) {
    $peers = $nodes - 1
    & "$PSScriptRoot\..\target\release\examples\lset.exe" --nodes $nodes --peers-per-node $peers --fanout 20 --time $time --lost-time-millis 1000 --result-file "$PSScriptRoot\..\..\results\lset"
    $primaries = $nodes / 100
    & "$PSScriptRoot\..\target\release\examples\lset.exe" --nodes $nodes --peers-per-node $peers --primaries $primaries --fanout 20 --time $time --lost-time-millis 1000 --result-file "$PSScriptRoot\..\..\results\lset"
}
