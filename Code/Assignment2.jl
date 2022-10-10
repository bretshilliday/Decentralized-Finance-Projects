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

# Takes the Dataframe of transactions and parses through them similulating trasactions to update wallet balances
function calculateBalances(df::DataFrame, wallets::Dict)
    for dfRow in eachrow(df)
        if !haskey(wallets, dfRow.topic1)
            wallets[dfRow.topic1] = [0,0,0]
        end
        if !haskey(wallets, dfRow.topic2)
            wallets[dfRow.topic2] = [0,0,0]
        end
        # Remove transaction amount from sender
        wallets[dfRow.topic1][1] -= dfRow.amount_sent
        wallets[dfRow.topic1][3] -= dfRow.amount_sent
        # Add transaction amount to receiver
        wallets[dfRow.topic2][2] += dfRow.amount_sent
        wallets[dfRow.topic2][3] += dfRow.amount_sent
    end
end

# Takes the dataframe and sorts the transactions as well as cleans up the data
function prepareDataframe(df::DataFrame)
    sort!(df, [:block_height, :tx_offset, :log_offset]) # Sorts the dataframe in place first by Block, then by Transaction Offset then finally by Log Offset so all transactions are in order
    filter!(:topic0 => t0 -> t0 == "DDF252AD1BE2C89B69C2B068FC378DAA952BA7F163C4A11628F55A4DF523B3EF", df)
    df.amount_sent = parse.(Int, df.data0, base=16) # Converts the amount sent in data0 to be a decimal number rather than a Hexidecimal String
    df.day = SubString.(df.signed_at, 1, 10) # Removes the datetime so we just have the date although its still a string
    return df
    #=
    Other Useful Things:
    - filter!(:amount => !=(0), df) # Remove rows where there is a 0 in the "amount" column
    - df.blockday = Date.(df.block_signed_at) # Create a new column in the dataframe which just contains the date derived from the datetime
    - df.amount = convert.(BigInt, df.amount) # Converts the amount from a Decimal to BigInt
    =#  
end

function groupByDay(df::DataFrame)
    gdf = groupby(df, :day)
    return gdf
end

function main(inputFile::String)
    inputDataframe = loadData(inputFile)
    cleanedData = prepareDataframe(inputDataframe)
    # printData(cleanedData, 10)
    wallets = Dict()
    calculateBalances(cleanedData, wallets)
    # print(first(wallets, 15))
    walletsFrame = DataFrame(wallet = [k for (k,v) in wallets], sent = [v[1] for (k,v) in wallets], received = [v[2] for (k,v) in wallets], net = [v[3] for (k,v) in wallets])
    # printData(walletsFrame, 10)

    gdf = groupByDay(cleanedData)
    dailyMetrics = DataFrame(day = String[], numberOfTransactions = Int[], volumeOfTransactions  = Int[])
    for sdf in gdf
        day = first(sdf).day
        numberTransactions = nrow(sdf)
        volumeTransactions = sum(sdf.amount_sent)
        push!(dailyMetrics, [day, numberTransactions, volumeTransactions])
    end
    CSV.write("/home/bret.shilliday/FNCE559.09/Homework/Assignment2/FNCE55909A2/CSV/USDCWalletInformation.csv", walletsFrame)
    CSV.write("/home/bret.shilliday/FNCE559.09/Homework/Assignment2/FNCE55909A2/CSV/DailyUSDCInformation.csv", dailyMetrics)
end

# -- Begin Executable -- 

# -- Variables --
inputFile = "/home/DefiClass2022/databases/t_token_A0B86991C6218B36C1D19D4A2E9EB0CE3606EB48.jld2"

# -- Code --
main(inputFile)