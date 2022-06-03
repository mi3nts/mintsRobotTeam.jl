using mintsRobotTeam
using Documenter

DocMeta.setdocmeta!(mintsRobotTeam, :DocTestSetup, :(using mintsRobotTeam); recursive=true)

makedocs(;
    modules=[mintsRobotTeam],
    authors="John Waczak",
    repo="https://github.com/mi3nts/mintsRobotTeam.jl/blob/{commit}{path}#{line}",
    sitename="mintsRobotTeam.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://mi3nts.github.io/mintsRobotTeam.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/mi3nts/mintsRobotTeam.jl",
    devbranch="main",
)
