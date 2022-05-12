#!/usr/bin/env julia
#
# Generate mean temperature, taken as the average of tmax and tmin. Low-memory
# and fast-running version.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

using ArgParse
using CodecZlib
using Printf
using ProgressMeter

DEFAULT_TMAX_FILENAME = "mean_tmax.csv.gz"
DEFAULT_TMIN_FILENAME = "mean_tmin.csv.gz"
DEFAULT_TMEAN_FILENAME = "mean_tmean.csv.gz"

function generate_tmean(tmin_path::String, tmax_path::String, output_path::String)
    temp_path = "$output_path.part"
    
    tmin_stream = GzipDecompressorStream(open(tmin_path, "r"))
    tmax_stream = GzipDecompressorStream(open(tmax_path, "r"))
    tmean_stream = GzipCompressorStream(open(temp_path, "w"))

    id_column = split(readline(tmin_stream), ",")[1]
    readline(tmax_stream)

    progress = ProgressUnknown("Generating $output_path:")
    write(tmean_stream, "$id_column,date,tmean\n")
    for tmin_line in eachline(tmin_stream)
        tmin_split = split(tmin_line, ",")
        tmax_split = split(readline(tmax_stream), ",")
        tmean = (parse(Float32, tmin_split[3]) + parse(Float32, tmax_split[3])) / 2
        write(tmean_stream, "$(tmin_split[1]),$(tmin_split[2]),$tmean\n")
        ProgressMeter.next!(progress)
    end
    ProgressMeter.finish!(progress)
    
    mv(temp_path, output_path)
end

arg_settings = ArgParseSettings()
@add_arg_table arg_settings begin
    "--tmin-file", "-t"
    "--tmax-file", "-T"
    "--output", "-o"
end

args = parse_args(arg_settings)

if all(values(args) .!= nothing)
    generate_tmean(args["tmin-file"], args["tmax-file"], args["output"])
else
    for geography_name in readdir("output/aggregated-combined/")
        aggregated_directory = "output/aggregated-combined/$geography_name"       
        extra_directory = "output/aggregated-combined/$geography_name"
        println(aggregated_directory)
    
        tmax_path = "$aggregated_directory/$DEFAULT_TMAX_FILENAME"
        tmin_path = "$aggregated_directory/$DEFAULT_TMIN_FILENAME"
        tmean_path = "$extra_directory/$DEFAULT_TMEAN_FILENAME"
        
        for file in [tmax_path, tmin_path]
            if ! isfile(file)
                throw(ErrorException("File $file does not exist"))
            end
        end
        
        if isfile(tmean_path)
            println("Skipping $tmean_path")
        else
            generate_tmean(tmin_path, tmax_path, tmean_path)
        end
    end
end
