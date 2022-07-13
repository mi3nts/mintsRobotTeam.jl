using Plots
using mintsRobotTeam
using ProgressMeter
using DataFrames, CSV
using BenchmarkTools
using georectification


include("../config.jl")

dates = ["11-23",
         "12-09",
         "12-10",
         "03-24",
         ]

rawPath = "/media/john/HSDATA/raw/"


outpath = "/media/john/HSDATA/processed/"

# make individual processed folders
for path ∈ [joinpath(outpath, date) for date ∈ dates]
    if !isdir(path)
        mkdir(path)
    end end

boatpaths = ["/media/john/HSDATA/boat/20201123",
             "/media/john/HSDATA/boat/20201209",
             "/media/john/HSDATA/boat/20201210",
             "/media/john/HSDATA/boat/20220324",
             ]


boatcsvpaths = [joinpath(outpath, date, "boat") for date ∈ dates]

rawPaths = [joinpath(rawPath, d) for d ∈ dates]


# # test it out:
# getBilFiles(rawPaths[1], "Scotty_1")
# getBilFiles(rawPaths[1], "Scotty_2")

# getBilFiles(rawPaths[2], "NoDye_1")
# getBilFiles(rawPaths[2], "Dye_1")

# testBilList = getBilFiles(rawPaths[1], "Scotty_1")


# bilhdr, times, spec, spechdr, lcf = getRawFileList(testBilList[1])


# processBilFile(
#                 testBilList[1],
#                 "../calibration",
#                 wavelengths,
#                 location_data["scotty"]["z"],
#                 θ_view,
#                 true,
#                 6,
#                 outpath,
#                 "Scotty_1"
#               )



# # try loading it in
# test_df_path = "/media/john/HSDATA/processed/11-23/Scotty_1/Scotty_1-1.csv"
# beenGeorectified(testBilList[1], outpath, "Scotty_1")




file_ids_11_23 = ["Scotty_1",  "Scotty_2", "Scotty_3", "Scotty_4", "Scotty_5"]
file_ids_12_09 = ["NoDye_1", "NoDye_2", "Dye_1", "Dye_2"]
file_ids_12_10 = ["NoDye_1", "NoDye_2", "Dye_1", "Dye_2"]
file_ids_03_24 = ["Demonstration", "Demonstration_long"]




# batch_georectify(rawPaths[1], outpath, file_ids_11_23)
# batch_georectify(rawPaths[2], outpath, file_ids_12_09)
# batch_georectify(rawPaths[3], outpath, file_ids_12_10)
# batch_georectify(rawPaths[4], outpath, file_ids_03_24)



# processBoatFiles(boatpaths[1], outpath)




# processAllBoatFiles(boatpaths, outpath, dates)


# outboatpaths = [joinpath(outpath, d, "boat") for d ∈ dates]
# isdir(boatpaths[1])

# makeTargets(outboatpaths, "scotty", 6)


processedpaths = [joinpath(outpath, d) for d ∈ dates]


imagepath = "/media/john/HSDATA/raw"
dates
flightsDict = Dict("11-23" => ("Scotty_1",
                               "Scotty_2",
                               "Scotty_3",
                               "Scotty_4",
                               "Scotty_5",
                               ),
                   "12-09" => ("NoDye_1",
                               "NoDye_2",
                               "Dye_1",
                               "Dye_2",
                               ),
                   "12-10" => ("NoDye_1",
                               "NoDye_2",
                               "Dye_1",
                               "Dye_2",
                               ),
                   "03-24" => ("Demonstration",
                               "Demonstration_long"
                               ))

master_lcfs = Dict()
for d ∈ dates
    dfs =[]
    for name ∈ flightsDict[d]
        lcf_df = masterLCF(joinpath(imagepath, d), name)
        push!(dfs, lcf_df)
    end
    master_lcfs[d] = vcat(dfs...)
end

master_lcfs

names(master_lcfs["11-23"])

# generate category labels
for d ∈ dates
    categories!(master_lcfs[d])
end


cats = categorySummaries(master_lcfs["11-23"])
nrow(cats)

# loop through the dates and update the Targets.csv files
@showprogress for d ∈ dates
    cats = categorySummaries(master_lcfs[d])
    boatpath = joinpath(outpath, d, "boat", "Targets.csv")
    boat_categories!(boatpath, cats)
    predye_postdye!(boatpath, d)
end



data_dirs = [joinpath(outpath, d) for d ∈ dates]

test_hsi_df = CSV.File(joinpath(outpath, "11-23", "Scotty_1", "Scotty_1-1.csv")) |> DataFrame
names(test_hsi_df)
ignore_cols = ["longitude", "latitude", "utc_times", "ilat", "ilon", "pixeltimes"]
data_cols = [n for n ∈ names(test_hsi_df) if !(n∈ignore_cols)]
"pixeltimes" ∈ data_cols


combineTargetsAndFeatures(joinpath(outpath, "11-23"), file_ids_11_23);
combineTargetsAndFeatures(joinpath(outpath, "12-09"), file_ids_12_09);
combineTargetsAndFeatures(joinpath(outpath, "12-10"), file_ids_12_10);
combineTargetsAndFeatures(joinpath(outpath, "03-24"), file_ids_03_24);


