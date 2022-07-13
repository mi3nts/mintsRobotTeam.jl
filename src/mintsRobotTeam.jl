module mintsRobotTeam


using ProgressMeter
using Plots
using georectification
using mintsOtter
using DataFrames, CSV
using Dates
using DataInterpolations
using Statistics

include("config.jl")

export getBilFiles
export getRawFileList
export processBilFile
export beenGeorectified
export batch_georectify
export processBoatFiles
export processAllBoatFiles
export makeTarget
export makeTargets
export categories!
export categorySummaries
export boat_categories!
export predye_postdye!
export combineTargetsAndFeatures
export getFileList


"""
    getBilFiles(dir, file_id)

Get a list of all .bil files in `dir` whose filename matches `file_id`. Returned list is sorted by the capture number at end of filename string.
"""
function getBilFiles(dir::String, file_id::String)
    bils = []
    for (root, dirs, files) ∈ walkdir(dir)
        for file ∈ files
            ffull = joinpath(root, file)
            namebase = split(split(ffull, "/")[end-1], "-")[1]

            if endswith(file, ".bil") && file_id == namebase
                push!(bils, joinpath(root, file))
            end
        end
    end

    # get list of file numbers to produce sorting indices

    endings = [split(f, "_")[end] for f ∈ bils]
    number = [lpad(split(f, "-")[1], 2, "0") for f ∈ endings]

    idx = sortperm(number)

    return bils[idx]
end




"""
    getRawFileList(bilpath)

Given a bil path, return the full list of paths to files needed for georectification.
"""
function getRawFileList(bilpath::String)
    basepath = "/"*joinpath(split(bilpath, "/")[1:end-1]...)
    bilhdrpath = ""
    timespath = ""
    specpath = ""
    spechdrpath = ""
    lcfpath = ""

    for f ∈ readdir(basepath)
        if endswith(f, ".bil.hdr")
            bilhdrpath = joinpath(basepath, f)
        elseif endswith(f, ".lcf")
            lcfpath = joinpath(basepath, f)
        elseif endswith(f, ".times")
            timespath = joinpath(basepath, f)
        elseif endswith(f, ".spec")
            specpath = joinpath(basepath, f)
        elseif endswith(f, ".spec.hdr")
            spechdrpath = joinpath(basepath, f)
        else
            continue
        end
    end

    return bilhdrpath, timespath, specpath, spechdrpath, lcfpath
end






"""
function processBilFile(bilpath::String,
                        calibrationpath::String,
                        λs::Array{Float64},
                        z_g::Float64,
                        θ::Float64,
                        isFlipped::Bool,
                        ndigits::Int,
                        outpath::String,
                        file_id::String;
                        compress=true
                        )

For a specified bil path, perform georectification and save the output.
"""
function processBilFile(bilpath::String,
                        calibrationpath::String,
                        λs::Array{Float64},
                        z_g::Float64,
                        θ::Float64,
                        isFlipped::Bool,
                        ndigits::Int,
                        outpath::String,
                        file_id::String;
                        compress=true
                        )
    # 1. get neccesary files
    base = split(bilpath, "/")[end-1]
    date = split(bilpath, "/")[end-2]

    bilhdrpath, timespath, specpath, spechdrpath, lcfpath = getRawFileList(bilpath)

    # 2. georectify
    df = georectify(bilpath,
                    bilhdrpath,
                    timespath,
                    specpath,
                    spechdrpath,
                    calibrationpath,
                    lcfpath,
                    λs,
                    z_g,
                    θ,
                    isFlipped,
                    ndigits,
                    )

    println("\tCleaning up memory")
    # 3.clean up memory
    GC.gc()
    ccall(:malloc_trim, Cvoid, (Cint,), 0)
    GC.gc()

    println("\tConverting to float")
    # 4. Convert to float
    for col ∈ eachcol(df[!, Not([:utc_times])])
        col = 1.0 .* col
    end

    # convert datetime to string
    format = "yyyy-mm-dd HH:MM:SS.sss"
    df.utc_times = Dates.format.(df.utc_times, format)

    # # create output
    if !isdir(joinpath(outpath, date, file_id))
        mkdir(joinpath(outpath, date, file_id))
    end


    println("\tSaving", joinpath(outpath, date, base*".csv"))


    if compress
        # we can compress the files too!
        CSV.write(joinpath(outpath, date, file_id, base*".csv"), df, compress=true)
    else
        CSV.write(joinpath(outpath, date, file_id, base*".csv"), df)
    end

    println("\tSuccess!")
    return df
end





"""
    beenGeorectified(bilpath::STring, outpath::String)

Check whether an HSI has already been georectified.
"""
function beenGeorectified(bilpath::String, outpath::String, file_id::String)
    base = split(bilpath, "/")[end-1]
    date = split(bilpath, "/")[end-2]

    if isfile(joinpath(outpath, date, file_id, base*".csv"))
        return true
    else
        return false
    end
end





"""
    function batch_georectify(basepath::String, outpath::String, file_ids=Vector{String})

Georectify all .bil files within `basepath` that match `file_ids`. Processed files are then saved to `outpath`.
"""
function batch_georectify(basepath::String, outpath::String, file_ids=Vector{String})
    for file_id ∈ file_ids
        println("Working on $(file_id)")
        bilfiles = getBilFiles(basepath, file_id)
        @showprogress for bilfile ∈ bilfiles
            if !beenGeorectified(bilfile, outpath, file_id)
                try
                    processBilFile(
                        bilfile,
                        "../calibration",
                        wavelengths,
                        location_data["scotty"]["z"],
                        θ_view,
                        true,
                        6,
                        outpath,
                        file_id;
                        compress = false,
                    )
                catch e
                    println(e)
                end
            end
        end
    end
end






"""
    processBoatFiles(basepath::String, outpath::String)

Give a `basepath` find all boat files and generate CSVs, saving them to `outpath`.
"""
function processBoatFiles(basepath::String, outpath::String)
    for (root, dirs, files) in walkdir(basepath)
        @showprogress for file in files
            if !(occursin("fixed", file))
                if occursin("AirMar", file)
                    println(file)
                    name = split(file, "_")[2]
                    airmar_gps, airmar_speed = importAirMar(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_airmar_gps.csv"), airmar_gps)
                    CSV.write(joinpath(outpath, name*"_airmar_speed.csv"), airmar_speed)
                elseif occursin("COM1", file)
                    println(file)
                    name = split(file, "_")[2]
                    COM1 = importCOM1(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_COM1.csv"), COM1)

                elseif occursin("COM2", file)
                    println(file)
                    name = split(file, "_")[2]
                    COM2 = importCOM2(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_COM2.csv"), COM2)

                elseif occursin("COM3", file)
                    println(file)
                    name = split(file, "_")[2]
                    COM3 = importCOM3(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_COM3.csv"), COM3)

                elseif occursin("LISST", file)
                    println(file)
                    name = split(file, "_")[2]
                    LISST = importLISST(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_LISST.csv"), LISST)

                elseif occursin("nmea", file) || occursin("NMEA", file)
                    println(file)
                    name = split(file, "_")[2]
                    nmea = importNMEA(joinpath(root, file))
                    CSV.write(joinpath(outpath, name*"_nmea.csv"), nmea)
                elseif occursin("nmea", file)
                    println(file)
                end
            end
        end
    end
end




"""
    processAllBoatFiles(paths::Array{String}, outpath::String, dates::Array{String})

For each path in `paths`, generate csv's from boat data. Used `dates` to generate output file names.
"""
function processAllBoatFiles(paths::Array{String}, outpath::String, dates::Array{String})
    for i ∈ 1:length(paths)
        out = joinpath(outpath, dates[i], "boat")
        if !isdir(out)
            mkdir(out)
        end

        try
            processBoatFiles(paths[i], out)
        catch e
            println(e)
        end
    end
end





"""
    makeTarget(basepath::String, locationName::String, ndigits::Int)

Given a path to boatfiles, `basepath`, generate csv of Target variables within the bounding box for `locationName` and with ilat and ilon set to `ndigits`.
"""
function makeTarget(basepath::String, locationName::String, ndigits::Int)
    # collect list of all CSVs for each sensor so we can join them
    airmar_gps_dfs = []
    airmar_speed_dfs = []
    com1_dfs = []
    com2_dfs = []
    com3_dfs = []
    lisst_dfs = []
    nmea_dfs = []

    println("\tCollecting boat files")
    @showprogress for f ∈ readdir(basepath)
        if endswith(f, "gps.csv")
            push!(airmar_gps_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "speed.csv")
            push!(airmar_speed_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "COM1.csv")
            push!(com1_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "COM2.csv")
            push!(com2_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "COM3.csv")
            push!(com3_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "LISST.csv")
            push!(lisst_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        elseif endswith(f, "nmea.csv")
            push!(nmea_dfs, DataFrame(CSV.File(joinpath(basepath, f))))
        end
    end

    # join df's and sort by utc time
    println("\tSorting by utc time")
    println("\tairmar_gps_df")
    airmar_gps_df = vcat(airmar_gps_dfs...)
    sort!(airmar_gps_df, :utc_dt)
    unique!(airmar_gps_df)

    println("\tairmar_speed_df")
    airmar_speed_df = vcat(airmar_speed_dfs...)
    sort!(airmar_speed_df, :utc_dt)
    unique!(airmar_speed_df)

    println("\tcom1_df")
    com1_df = vcat(com1_dfs...)
    sort!(com1_df, :utc_dt)
    unique!(com1_df)

    println("\tcom2_df")
    com2_df = vcat(com2_dfs...)
    sort!(com2_df, :utc_dt)
    unique!(com2_df)

    println("\tcom3_df")
    com3_df = vcat(com3_dfs...)
    sort!(com3_df, :utc_dt)
    unique!(com3_df)

    println("\tlisst_df")
    lisst_df = vcat(lisst_dfs...)
    sort!(lisst_df, :utc_dt)
    unique!(lisst_df)

    println("\tnmea_df")
    nmea_df = vcat(nmea_dfs...)
    sort!(nmea_df, :utc_dt)
    nmea_df = unique(nmea_df)
    nmea_df = combine(first, groupby(sort(nmea_df, :utc_dt), :utc_dt))


    # bounding box for Scotty's Ranch
    println("\tFiltering to within location bounding box")
    w = location_data[locationName]["w"]
    n = location_data[locationName]["n"]
    s = location_data[locationName]["s"]
    e = location_data[locationName]["e"]

    # make sure we're in the correct bounding box
    nmea_df = nmea_df[(nmea_df.latitude .> s) .& (nmea_df.latitude .< n) .& (nmea_df.longitude .> w) .& (nmea_df.longitude .< e), :]

    # filter to times within boat GPS values
    tstart = nmea_df.utc_dt[1]
    tend = nmea_df.utc_dt[end]



    # filter df's to those times that fall within Boat GPS times (i.e. nmea)
    com1_df_filtered = com1_df[(com1_df.utc_dt .>= tstart) .& (com1_df.utc_dt .<= tend ), :]
    com2_df_filtered = com2_df[(com2_df.utc_dt .>= tstart) .& (com2_df.utc_dt .<= tend ), :]
    com3_df_filtered = com3_df[(com3_df.utc_dt .>= tstart) .& (com3_df.utc_dt .<= tend ), :]
    airmar_speed_df_filtered = airmar_speed_df[(airmar_speed_df.utc_dt .>= tstart) .& (airmar_speed_df.utc_dt .<= tend ), :]
    lisst_df_filtered = lisst_df[(lisst_df.utc_dt .>= tstart) .& (lisst_df.utc_dt .<= tend ), :]

    # now let's interpolate to match the nmea times
    println("\tInterpolating to match gps times")
    interpolated = Dict()

    interpolated["longitude"] = nmea_df.longitude
    interpolated["latitude"] = nmea_df.latitude
    interpolated["unix_dt"] = nmea_df.unix_dt
    interpolated["utc_dt"] = nmea_df.utc_dt


    # go through COM1
    println("\tInterpolating COM1")
    names(com1_df_filtered)
    Br_interp = CubicSpline(com1_df_filtered.Br, com1_df_filtered.unix_dt)
    interpolated["Br"] = Br_interp.(interpolated["unix_dt"])

    Ca_interp = CubicSpline(com1_df_filtered.Ca, com1_df_filtered.unix_dt)
    interpolated["Ca"] = Ca_interp.(interpolated["unix_dt"])

    Cl_interp = CubicSpline(com1_df_filtered.Cl, com1_df_filtered.unix_dt)
    interpolated["Cl"] = Cl_interp.(interpolated["unix_dt"])

    HDO_interp = CubicSpline(com1_df_filtered.HDO, com1_df_filtered.unix_dt)
    interpolated["HDO"] = HDO_interp.(interpolated["unix_dt"])

    HDO_percent_interp = CubicSpline(com1_df_filtered.HDO_percent, com1_df_filtered.unix_dt)
    interpolated["HDO_percent"] = HDO_percent_interp.(interpolated["unix_dt"])

    NH4_interp = CubicSpline(com1_df_filtered.NH4, com1_df_filtered.unix_dt)
    interpolated["NH4"] = NH4_interp.(interpolated["unix_dt"])

    NO3_interp = CubicSpline(com1_df_filtered.NO3, com1_df_filtered.unix_dt)
    interpolated["NO3"] = NO3_interp.(interpolated["unix_dt"])

    Na_interp = CubicSpline(com1_df_filtered.Na, com1_df_filtered.unix_dt)
    interpolated["Na"] = Na_interp.(interpolated["unix_dt"])

    Salinity3488_interp = CubicSpline(com1_df_filtered.Salinity3488, com1_df_filtered.unix_dt)
    interpolated["Salinity3488"] = Salinity3488_interp.(interpolated["unix_dt"])

    SpCond_interp = CubicSpline(com1_df_filtered.SpCond, com1_df_filtered.unix_dt)
    interpolated["SpCond"] = SpCond_interp.(interpolated["unix_dt"])

    TDS_interp = CubicSpline(com1_df_filtered.TDS, com1_df_filtered.unix_dt)
    interpolated["TDS"] = TDS_interp.(interpolated["unix_dt"])

    Temp3488_interp = CubicSpline(com1_df_filtered.Temp3488, com1_df_filtered.unix_dt)
    interpolated["Temp3488"] = Temp3488_interp.(interpolated["unix_dt"])

    Turb3488_interp = CubicSpline(com1_df_filtered.Turb3488, com1_df_filtered.unix_dt)
    interpolated["Turb3488"] = Turb3488_interp.(interpolated["unix_dt"])

    pH_interp = CubicSpline(com1_df_filtered.pH, com1_df_filtered.unix_dt)
    interpolated["pH"] = pH_interp.(interpolated["unix_dt"])

    pH_mV_interp = CubicSpline(com1_df_filtered.pH_mV, com1_df_filtered.unix_dt)
    interpolated["pH_mV"] = pH_mV_interp.(interpolated["unix_dt"])


    # go through COM2
    println("\tInterpolating COM2")
    names(com2_df_filtered)
    CDOM_interp = CubicSpline(com2_df_filtered.CDOM, com2_df_filtered.unix_dt)
    interpolated["CDOM"] = CDOM_interp.(interpolated["unix_dt"])

    Chl_interp = CubicSpline(com2_df_filtered.Chl, com2_df_filtered.unix_dt)
    interpolated["Chl"] = Chl_interp.(interpolated["unix_dt"])

    ChlRed_interp = CubicSpline(com2_df_filtered.ChlRed, com2_df_filtered.unix_dt)
    interpolated["ChlRed"] = ChlRed_interp.(interpolated["unix_dt"])

    Temp3489_interp = CubicSpline(com2_df_filtered.Temp3489, com2_df_filtered.unix_dt)
    interpolated["Temp3489"] = Temp3489_interp.(interpolated["unix_dt"])

    Turb3489_interp = CubicSpline(com2_df_filtered.Turb3489, com2_df_filtered.unix_dt)
    interpolated["Turb3489"] = Turb3489_interp.(interpolated["unix_dt"])

    bg_interp = CubicSpline(com2_df_filtered.bg, com2_df_filtered.unix_dt)
    interpolated["bg"] = bg_interp.(interpolated["unix_dt"])

    bgm_interp = CubicSpline(com2_df_filtered.bgm, com2_df_filtered.unix_dt)
    interpolated["bgm"] = bgm_interp.(interpolated["unix_dt"])


    # go through COM3
    println("\tInterpolating COM3")
    names(com3_df_filtered)
    CO_interp = CubicSpline(com3_df_filtered.CO, com3_df_filtered.unix_dt)
    interpolated["CO"] = CO_interp.(interpolated["unix_dt"])

    OB_interp = CubicSpline(com3_df_filtered.OB, com3_df_filtered.unix_dt)
    interpolated["OB"] = OB_interp.(interpolated["unix_dt"])

    RefFuel_interp = CubicSpline(com3_df_filtered.RefFuel, com3_df_filtered.unix_dt)
    interpolated["RefFuel"] = RefFuel_interp.(interpolated["unix_dt"])

    Salinity3490_interp = CubicSpline(com3_df_filtered.Salinity3490, com3_df_filtered.unix_dt)
    interpolated["Salinity3490"] = Salinity3490_interp.(interpolated["unix_dt"])

    TDS_interp = CubicSpline(com3_df_filtered.TDS, com3_df_filtered.unix_dt)
    interpolated["TDS"] = TDS_interp.(interpolated["unix_dt"])

    TRYP_interp = CubicSpline(com3_df_filtered.TRYP, com3_df_filtered.unix_dt)
    interpolated["TRYP"] = TRYP_interp.(interpolated["unix_dt"])

    Temp3490_interp = CubicSpline(com3_df_filtered.Temp3490, com3_df_filtered.unix_dt)
    interpolated["Temp3490"] = Temp3490_interp.(interpolated["unix_dt"])

    Turb3490_interp = CubicSpline(com3_df_filtered.Turb3490, com3_df_filtered.unix_dt)
    interpolated["Turb3490"] = Turb3490_interp.(interpolated["unix_dt"])


    # go through lisst
    println("\tInterpolating LISST")
    names(lisst_df_filtered)
    # cubic spline failing for some reason.
    SSC_interp = QuadraticInterpolation(lisst_df_filtered.SSC, lisst_df_filtered.unix_dt)
    interpolated["SSC"] = []
    for t ∈ interpolated["unix_dt"]
        try
            push!(interpolated["SSC"], SSC_interp(t))
        catch e
            push!(interpolated["SSC"], NaN)
        end
    end


    # generate ilat and ilon
    println("\tGenerating ilat and ilon")

    interpolated["ilat"] = round.(interpolated["latitude"], digits=ndigits)
    interpolated["ilon"] = round.(interpolated["longitude"], digits=ndigits)


    println("\tSaving CSV")

    CSV.write(joinpath(basepath, "Targets.csv"), DataFrame(interpolated))
end




"""
    makeTargets(basepaths::Array{String}, locationName::String, ndigits::Int)

Loop through `basepaths` and makeTarget for each path.
"""
function makeTargets(basepaths::Array{String}, locationName::String, ndigits::Int)
    for path ∈ basepaths
        println("Making targets for $(path)")
        makeTarget(path, locationName, ndigits)
    end
end



"""
    categories!(lcf_df::DataFrame)

Given a master_lcf dataframe, generate category names for each flight.
"""
function categories!(lcf_df::DataFrame)
    # create a new column for the category
    lcf_df.category = Array{String}(undef, size(lcf_df, 1))

    for row ∈ eachrow(lcf_df)
        splitRoot = split(row.files, "/")
        cat = split(splitRoot[end-1], "-")[1]
        row.category = cat
    end
end



"""
    categorySummaries(df::DataFrame)

Given a masterlcf dataframe `df` with categories, generate a summary dictionary for each unique category with it's global start and end time.
"""
function categorySummaries(df::DataFrame)
    gdf = groupby(df, :category)

    df = combine(gdf,
                 :tstart => minimum => :tstart,
                 :tend => maximum => :tend,
                 :category => first => :category,
                 )

    sort!(df, :tstart)
    return df
end



"""
    boat_categories!(path::String, cats_df::DataFrame)

Given a path to a boat file and the associated category summary df, `cats_df`, add a categories column to the boat data found at `path`.
"""
function boat_categories!(path::String, cats_df::DataFrame)
    boat_df = CSV.File(path) |> DataFrame
    sort!(boat_df, :utc_dt)

    # pre-allocate category array
    boat_df.category = ["missing" for _ ∈ 1:nrow(boat_df)]

    # generate category based on utc_time
    for row ∈ eachrow(boat_df)
        t = row.utc_dt

        for i ∈ 1:nrow(cats_df)
            if t < cats_df.tstart[i]
                row.category = cats_df.category[i] * "_preflight"
                break
            end

            if cats_df.tstart[i] <= t && t <= cats_df.tend[i]
                row.category = cats_df.category[i]
                break
            end
        end

        # if it's still "missing" then the time was after all the flights
        if row.category == "missing"
            row.category  = "postflights"
        end

    end

    CSV.write(path, boat_df)
    return boat_df
end




"""
    predye_postdye!(path::String)

Add column to data specifying when the point was collected either
- pre dye release
- post dye release
- ignore

"""
function predye_postdye!(path::String, date::String)
    boat_df = CSV.File(path) |> DataFrame
    sort!(boat_df, :utc_dt)


    # pre allocate with "ignore"
    boat_df.predye_postdye = ["ignore" for _ ∈ 1:nrow(boat_df)]

    if date ∈ keys(time_ranges)
        ts = time_ranges[date]

        for row ∈ eachrow(boat_df)
            t = row.utc_dt

            if t >= ts[1] && t <= ts[2]
                row.predye_postdye = "Pre-Dye"
            elseif t >= ts[2] && t <= ts[3]
                row.predye_postdye = "Post-Dye"
            else
                continue
            end
        end
    end

    CSV.write(path, boat_df)
end







"""
    getFileList(basepath::String, dirs=Array{String})

Get a list of all georectified HSI files from each dir in `dis`
"""
function getFileList(basepath::String, dirs=Array{String})
    file_list = []
    for dir ∈ dirs
        fs =[joinpath(basepath, dir, f) for f ∈ filter(x->endswith(x, ".csv"), readdir(joinpath(basepath, dir)))]

        fnames = [split(split(f, "/")[end], ".")[1] for f ∈ fs]
        number = [lpad(split(f, "-")[2], 2, "0") for f ∈ fnames]

        idx = sortperm(number)

        res = fs[idx]

        push!(file_list, res)
    end
    return vcat(file_list...)
end









function combineTargetsAndFeatures(basepath::String, dirs::Array{String})
    # load in the boat data
    println("Loading Targets.csv")
    targets = DataFrame(CSV.File(joinpath(basepath, "boat", "Targets.csv")))

    # group the boat data by ilat and ilon column
    println("Grouping by ilat/ilon")
    gdf = groupby(targets, [:ilat, :ilon])

    # aggregate into one dataframe, using the mean
    println("Aggregating data")
    df = combine(gdf,
                :Br => mean => :Br,
                :CDOM => mean => :CDOM,
                :CO => mean => :CO,
                :Ca => mean => :Ca,
                :Chl => mean => :Chl,
                :ChlRed => mean => :ChlRed,
                :Cl => mean => :Cl,
                :HDO => mean => :HDO,
                :HDO_percent => mean => :HDO_percent,
                :NH4 => mean => :NH4,
                :NO3 => mean => :NO3,
                :Na => mean => :Na,
                :OB => mean => :OB,
                :RefFuel => mean => :RefFuel,
                :SSC => mean => :SSC,
                :Salinity3488 => mean => :Salinity3488,
                :Salinity3490 => mean => :Salinity3490,
                :SpCond => mean => :SpCond,
                :TDS => mean => :TDS,
                :TRYP => mean => :TRYP,
                :Temp3488 => mean => :Temp3488,
                :Temp3489 => mean => :Temp3489,
                :Temp3490 => mean => :Temp3490,
                :Turb3488 => mean => :Turb3488,
                :Turb3489 => mean => :Turb3489,
                :Turb3490 => mean => :Turb3490,
                :bg => mean => :bg,
                :bgm => mean => :bgm,
                :latitude => mean => :latitude,
                :longitude => mean => :longitude,
                :pH => mean => :pH,
                :pH_mV => mean => :pH_mV,
                :unix_dt => mean => :unix_dt,
                :utc_dt => first => :utc_dt,
                :category => first => :category,
                :predye_postdye => first => :predye_postdye,
                )


    # now sort by the times
    println("Sorting by time stamp")
    sort!(df, :utc_dt)

    hsi_files = getFileList(basepath, dirs)

    # loop through each of the files in order and fill up our targets dataframe
    @showprogress for f ∈ hsi_files
        # read the hsi df
        hsi_df = CSV.File(f) |> DataFrame

        # generate list of columns we want
        ignore_cols = ["longitude", "latitude", "utc_times", "ilat", "ilon", "pixeltimes"]
        data_cols = [n for n ∈ names(hsi_df) if !(n∈ignore_cols)]

        # if these columns don't exist in our dataframe, pre-allocate to type missing
        if !("λ_1" ∈ names(df))
            for col ∈ data_cols
                df[!, col] = Union{Missing, Float64}[missing for i ∈ 1:nrow(df)]
            end
        end

        # first we compute max and min for rough bounding box
        latmax = maximum(hsi_df.ilat)
        latmin = minimum(hsi_df.ilat)

        lonmax = maximum(hsi_df.ilon)
        lonmin = minimum(hsi_df.ilon)

        # now we want to loop through all rows of our boat df and check to see if the ilat/ilon pairs from
        # the hsi_df match. If so, update the data in df
        for row ∈ eachrow(df)
            # only continue if the row doesn't yet have a match
            if ismissing(row["λ_1"])
                dfilat = row.ilat
                dfilon = row.ilon

                # make sure we're within the bounding box
                if (dfilat <= latmax) && (dfilat >= latmin) && (dfilon <= lonmax) && (dfilon >= lonmin)
                    # now that we know we're withing the crude bounding box, find indices in hsi_df for all matches
                    idx = vec(findall((hsi_df.ilat .== dfilat) .& (hsi_df.ilon .== dfilon)))

                    # make sure we do have a match
                    if size(idx, 1) == 1
                        idx = idx[1]
                        # copy the data over
                        for col ∈ data_cols
                            row[col] = hsi_df[idx, col]
                        end
                    end
                end
            end
        end
    end

    CSV.write(joinpath(basepath, "TargetsAndFeatures.csv"), df)
end










"""
    combineTargetsAndFeatures(basepath::String)

Loop over image files and generate new dataframe of boat samples collocated with image pixels.
"""
function testfunc(basepath::String)
    # loop through each HSI 
    println("Looping through image files")
    for (root, dirs, files) ∈ walkdir(basepath)
        for f ∈ files
            if endswith(f, "datacube.h5")
                h5open(joinpath(root, f), "r") do hsi
                    println("Working on ", joinpath(root, f))

                    data = read(hsi["data"])


                    # generate list of keys that we will include in final training set
                    ignore_keys = ["reflectance",
                                   "radiance",
                                   "resolutions",
                                   "ilat",
                                   "ilon",
                                   "times",
                                   "longitudes",
                                   "latitudes"]

                    derived_keys = [key for key ∈ keys(data) if !(key ∈ ignore_keys)]


                    # initialize new columns in df for incoming data
                    if !("λ_1" ∈ names(df))
                        # add columns for reflectance and radiance
                        for i ∈ 1:462
                            df[!, "λ_$(i)"] = Union{Missing, Float64}[missing for i ∈ 1:size(df)[1]]
                        end
                        for i ∈ 1:462
                            df[!, "λ_$(i)_rad"] = Union{Missing, Float64}[missing for i ∈ 1:size(df)[1]]
                        end

                        # add columns for derived quantities
                        for key ∈ derived_keys
                            df[!, key] = Union{Missing, Float64}[missing for i ∈ 1:size(df)[1]]
                        end

                    end


                    # dc_ilat = read(data["ilat"])
                    # dc_ilon = read(data["ilon"])
                    dc_ilat = data["ilat"]
                    dc_ilon = data["ilon"]

                    # compute maximum and minima for rough bounding box
                    latmax = maximum(dc_ilat)
                    latmin = minimum(dc_ilat)

                    lonmax = maximum(dc_ilon)
                    lonmin = minimum(dc_ilon)


                    println("Beginning inner loop")
                    #@showprogress for i ∈ 1:size(df)[1]
                    @showprogress for row ∈ eachrow(df)
                        # first make sure that we haven't already added the values
                        if ismissing(row["λ_1"])
                            dfilat = row.ilat
                            dfilon = row.ilon
                            # make sure we're within the bounding box
                            if (dfilat <= latmax) && (dfilat >= latmin) && (dfilon <= lonmax) && (dfilon >= lonmin)
                                # check that ilat and ilon match
                                idx = vec(findall((dc_ilat .== dfilat) .& (dc_ilon .== dfilon)))
                                if size(idx)[1] > 0
                                    # update df with average of matched ilat and ilon vals


                                    ref = mean(data["reflectance"][idx, :], dims=1)
                                    rad = mean(data["radiance"][idx, :], dims=1)



                                    Λs = [Symbol("λ_$(i)") for i∈1:462]
                                    Λs_rad = [Symbol("λ_$(i)_rad") for i∈1:462]

                                    # for i ∈ 1:462
                                    #     row["λ_$(i)"] = ref[i]
                                    #     row["λ_$(i)_rad"] = rad[i]
                                    # end


                                    row[Λs] .= ref[:]
                                    row[Λs_rad] .= rad[:]

                                    # loop over derive quantities
                                    for key ∈ derived_keys
                                        key_mean = mean(data[key][idx])
                                        row[key] = key_mean
                                    end

                                end
                            end
                        end
                    end
                end
            end
        end
    end
    CSV.write(joinpath(basepath, "TargetsAndFeatures.csv"), df)



end



end
