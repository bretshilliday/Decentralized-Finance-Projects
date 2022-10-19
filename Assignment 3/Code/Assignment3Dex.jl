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


function topic_standard(target_topic)
    uppercase(target_topic)[(end-63):end]
end

# Takes the dataframe and sorts the transactions as well as cleans up the data
function prepareData(df::DataFrame)
    # df.amount_sent = parse.(Int, df.data0, base=16) # Converts the amount sent in data0 to be a decimal number rather than a Hexidecimal String
    return df
    #=
    Other Useful Things:
    - filter!(:amount => !=(0), df) # Remove rows where there is a 0 in the "amount" column
    - df.blockday = Date.(df.block_signed_at) # Create a new column in the dataframe which just contains the date derived from the datetime
    - df.amount = convert.(BigInt, df.amount) # Converts the amount from a Decimal to BigInt
    - df.day = SubString.(df.signed_at, 1, 10) # Removes the datetime so we just have the date although its still a string
    =#  
end

function getPool(df, pool)
    filter!(:log_emitter => le -> le == pool, df)
end

function getEvents(df, event)
    filter!(:topic0 => t0 -> t0 == event, df)
end

function calculatePriceAtSwap(df)
    
end

function sortChrono(df)
    sort!(df, [:block_height, :tx_offset, :log_offset]) # Sorts the dataframe in place first by Block, then by Transaction Offset then finally by Log Offset so all transactions are in order
end

function main(inputFile::String, event::String, pools)
    inputDataframe = loadData(inputFile)
    for pool in pools
        poolFrame = copy(inputDataframe)
        # Clean up data
        # prepareData(poolFrame)
        # Get just that pool
        getPool(poolFrame, pool)
        # Get just the swap events
        # getEvents(poolFrame, event)
        # Calculate the price at each swap
        # calculatePriceAtSwap(poolFrame)
        # Sort cronologically
        # sortChrono(poolFrame)
        # Export the time data with the price for plotting
        printData(poolFrame, 5)
        # CSV.write("/home/bret.shilliday/Decentrailized Finance Projects/Assignments/Assignment 3/CSV/$pool.csv", poolFrame)
    end
end

# -- Begin Executable -- 

# -- Variables --
inputFile = "/home/DefiClass2022/databases/dexes/dexes_2022_1.jld2"
pools = ["7BEA39867E4169DBE237D55C8242A8F2FCDCC387", "8AD599C3A0FF1DE082011EFDDC58F1908EB6E6D8", "88E6A0C2DDD26FEEB64F039A2C41296FCB3F5640", "B4E16D0168E52D35CACD2C6185B44281EC28C9DC"] # [USDC 1, USDC 2, USDC 3]
event = "C42079F94A6350D7E6235F29174924F928CC2AC818EB64FED8004E115FBCCA67" # Swap Event

# -- Code --
main(inputFile, event, pools)


# Safekeeping
# Original ["0x7bea39867e4169dbe237d55c8242a8f2fcdcc387", "8ad599c3a0ff1de082011efddc58f1908eb6e6d8", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]