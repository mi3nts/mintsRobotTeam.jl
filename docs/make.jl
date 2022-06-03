using mintsRobotTeam
using Documenter

DocMeta.setdocmeta!(mintsRobotTeam, :DocTestSetup, :(using mintsRobotTeam); recursive=true)

makedocs(;
    modules=[mintsRobotTeam],
    authors="John Waczak",
    repo="https://github.com/john-waczak/mintsRobotTeam.jl/blob/{commit}{path}#{line}",
    sitename="mintsRobotTeam.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://john-waczak.github.io/mintsRobotTeam.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/john-waczak/mintsRobotTeam.jl",
    devbranch="main",
)
