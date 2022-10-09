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

# Prints the first numRows rows of the dataframe in its entirety to the terminal
function printData(df, numRows)
    show(first(df, numRows), allcols=true, allrows=true, truncate = 0)
    println("\n")
end

# Takes the dataframe and sorts the transactions as well as cleans up the data
function prepareDataframe(df::DataFrame)
    sort!(df, [:block_height, :tx_offset, :logoffset]) # Sorts the dataframe in place first by Block, then by Transaction Offset then finally by Log Offset so all transactions are in order
    filter!(:amount => !=(0), df) # Removes transactions where $0 is being sent as this does not make sense although could technically happen
    df.blockday = Date.(df.block_signed_at) # Create a new column in the dataframe which just contains the date derived from the datetime
    df.amount = convert.(BigInt, df.amount) # Converts the amount from a Decimal to BigInt
    return df
end


function main(inputFile::String)
    inputDataframe = loadData(inputFile)
    printData(inputDataframe, 10)
end


# -- Begin Executable -- 

# -- Variables --
inputFile = "/home/DefiClass2022/databases/t_token_A0B86991C6218B36C1D19D4A2E9EB0CE3606EB48.jld2"

# -- Code --
main(inputFile)

