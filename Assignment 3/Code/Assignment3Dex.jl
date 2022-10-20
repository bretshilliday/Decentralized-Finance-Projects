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
    df.data0 = parse.(BigInt, df.data0, base=16)
    df.data1 = parse.(BigInt, df.data1, base=16)
    df.data2 = parse.(BigInt, df.data2, base=16)
    df.data3 = parse.(BigInt, df.data3, base=16)
    df.price .= BigFloat(0)
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
    for row in eachrow(df)
        USDCamount = row.data0 - row.data2
        ETHamount = row.data1 - row.data3
        row.price = -(BigFloat(USDCamount/ETHamount) * BigFloat(1000000000000))
        # if (row.data0 == 0)
        #     row.price = BigFloat(row.data2/row.data1) * BigFloat(1000000000000)
        # else
        #     row.price = BigFloat(row.data0/row.data3) * BigFloat(1000000000000)
        # end
    end
end

function sortChrono(df)
    sort!(df, [:block_height, :tx_offset, :log_offset]) # Sorts the dataframe in place first by Block, then by Transaction Offset then finally by Log Offset so all transactions are in order
end

function main(inputDF, event::String, pools)
    for pool in pools
        poolFrame = copy(inputDF)
        # Get just that pool
        getPool(poolFrame, pool)
        # Get just the swap events
        getEvents(poolFrame, event)
        # Clean up data
        prepareData(poolFrame)
        # Calculate the price at each swap
        calculatePriceAtSwap(poolFrame)
        # Sort cronologically
        sortChrono(poolFrame)
        # Export the time data with the price for plotting
        CSV.write("/home/bret.shilliday/Decentrailized Finance Projects/Assignments/Assignment 3/CSV/$pool.csv", select(poolFrame, [:tx_hash, :topic1, :topic2, :signed_at, :data0, :data1, :data2, :data3, :price]))
    end
end

# -- Begin Executable -- 

# -- Variables --
inputFile = "/home/DefiClass2022/databases/dexes/dexes_2022_1.jld2"
pools = ["397FF1542F962076D0BFE58EA045FFA2D347ACA0", "B4E16D0168E52D35CACD2C6185B44281EC28C9DC"] # Refer to Notes for what these values are
event = "D78AD95FA46C994B6551D0DA85FC275FE613CE37657FB8D5E3D130840159D822" # Swap Event

# -- Code --
bigFrame = loadData("/home/DefiClass2022/databases/dexes/dexes_2022_1.jld2")
for i in 2:10
    println(i)
    newFrame = loadData("/home/DefiClass2022/databases/dexes/dexes_2022_$i.jld2")
    append!(bigFrame, newFrame)
end
main(bigFrame, event, pools)


# Safekeeping
# Original ["0x7bea39867e4169dbe237d55c8242a8f2fcdcc387", "8ad599c3a0ff1de082011efddc58f1908eb6e6d8", "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"]