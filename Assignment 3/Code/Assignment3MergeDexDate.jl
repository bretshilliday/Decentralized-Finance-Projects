using Pkg
using LibPQ
using Tables
using DataFrames
using CSV
using JLD2, FileIO
using Decimals
using Plots
using Statistics
using InvertedIndices
using SharedArrays
using JSON
using ColorSchemes
using TableView
using Dates
using GLM
using TexTables
using Latexify
using TexTables
using Formatting
using VegaLite
using DataStructures

# Loads the data from file
function loadData(data::String)
    println("Loading Data ...")
    RawDF = load_object(data)
    println("Number Objects Loaded: $(format(nrow(RawDF), commas=true))")
    return RawDF
end

function main()
    bigFrame = loadData("/home/DefiClass2022/databases/dexes/dexes_2022_1.jld2")
    for i in 2:10
        println(i)
        newFrame = loadData("/home/DefiClass2022/databases/dexes/dexes_2022_$i.jld2")
        append!(bigFrame, newFrame)
    end
    # save_object("/home/bret.shilliday/Decentrailized Finance Projects/Assignments/Assignment 3/Data/fulldatabase.jld2", bigFrame)
    # CSV.write("/home/bret.shilliday/Decentrailized Finance Projects/Assignments/Assignment 3/CSV/$pool.csv", select(poolFrame, [:tx_hash, :signed_at, :data0, :data1, :data2, :data3, :price]))
end

# -- Begin Executable -- 

# -- Code --
main()
