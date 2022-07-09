using Plots
using mintsRobotTeam
using ProgressMeter
using DataFrames, CSV
using BenchmarkTools



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
    end
end

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




# file_ids_11_23 = ["Scotty_1",  "Scotty_2", "Scotty_3", "Scotty_4", "Scotty_5"]
# file_ids_12_09 = ["NoDye_1", "NoDye_2", "Dye_1", "Dye_2"]
# file_ids_12_10 = ["NoDye_1", "NoDye_2", "Dye_1", "Dye_2"]
# file_ids_03_24 = ["Demonstration", "Demonstration_long"]





# batch_georectify(rawPaths[1], outpath, file_ids_11_23)
# batch_georectify(rawPaths[2], outpath, file_ids_12_09)
# batch_georectify(rawPaths[3], outpath, file_ids_12_10)
# batch_georectify(rawPaths[4], outpath, file_ids_03_24)



# processBoatFiles(boatpaths[1], outpath)




# processAllBoatFiles(boatpaths, outpath, dates)















# function main()
#     # # 1. georectify HSIs
#     # batch_georectify(dates, rawPath, outpath)

#     # # 2. collect boat CSVs
#     # processAllBoatFiles(boatpaths, dates)


#     outpath = "/home/john/gitRepos/mintsRobotTeam/influxdb/data"
#     for path ∈ [joinpath(outpath, date) for date ∈ dates]
#         if !isdir(path)
#             mkdir(path)
#         end
#     end

#     dates = ["03-23",
#              "03-24",
#              ]

#     boatpaths = ["/home/john/gitRepos/mintsRobotTeam/influxdb/data/20220323",
#                  "/home/john/gitRepos/mintsRobotTeam/influxdb/data/20220324",
#                  ]

#     # 2. collect boat CSVs
#     processAllBoatFiles(boatpaths, dates)


# end






