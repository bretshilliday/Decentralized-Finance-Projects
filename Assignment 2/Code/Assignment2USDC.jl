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

# Given a topic0 in the form 0xabc......123, this will convert it into a ABC...123 value as this is how topic0's are stored in the database
function topic_standard(target_topic)
    uppercase(target_topic)[(end-63):end]
end

# Takes the dataframe of transactions and parses through them similulating trasactions to update wallet balances
function calculateBalances(df::DataFrame, wallets::Dict)
    for dfRow in eachrow(df) # Goes through each row in the dataframe
        if !haskey(wallets, dfRow.topic1) # True if the sender of the transaction is a wallet we have not yet seen
            wallets[dfRow.topic1] = [0,0,0] # Add the wallet to the dictionary with a 0 balance
        end
        if !haskey(wallets, dfRow.topic2) # True if the receiver of the transaction is a wallet we have not yet seen
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
    filter!(:topic0 => t0 -> t0 == "DDF252AD1BE2C89B69C2B068FC378DAA952BA7F163C4A11628F55A4DF523B3EF", df) # Filter by only the topic we want
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

# Groups transactions by day
function groupByDay(df::DataFrame)
    gdf = groupby(df, :day)
    return gdf
end

# Main function to be run
function main(inputFile::String)
    inputDataframe = loadData(inputFile) # Load in our database
    cleanedData = prepareDataframe(inputDataframe) # Clean up the data and get it ready for analysis
    wallets = Dict() # Create a dictionary to keep track of wallets and associated values
    calculateBalances(cleanedData, wallets) # Simulate transactions of the wallets so we can see which wallets sent more than recieved during the time period (we will keep track of the amount sent, recieved and the difference)
    walletsFrame = DataFrame(wallet = [k for (k,v) in wallets], sent = [v[1] for (k,v) in wallets], received = [v[2] for (k,v) in wallets], net = [v[3] for (k,v) in wallets]) # Converts the dictionary into a dataframe

    gdf = groupByDay(cleanedData) # Groups the data into days
    dailyMetrics = DataFrame(day = String[], numberOfTransactions = Int[], volumeOfTransactions  = Int[]) # Create a summary dataframe
    for sdf in gdf # Now that we have all the data grouped into daily subdataframes, we need to look at each daily subframe to calculate some metrics
        day = first(sdf).day # Get the day of the subdataframe
        numberTransactions = nrow(sdf) # Get the number of transactions that day
        volumeTransactions = sum(sdf.amount_sent) # Get the volume ($ amount) of transactions that day
        push!(dailyMetrics, [day, numberTransactions, volumeTransactions]) # Add them to the summary dataframe
    end
    # Write this to a CSV
    CSV.write("/home/bret.shilliday/FNCE559.09/Homework/Assignment2/FNCE55909A2/CSV/USDCWalletInformation.csv", walletsFrame)
    CSV.write("/home/bret.shilliday/FNCE559.09/Homework/Assignment2/FNCE55909A2/CSV/DailyUSDCInformation.csv", dailyMetrics)
end

# -- Begin Executable -- 

# -- Variables --
inputFile = "/home/DefiClass2022/databases/t_token_A0B86991C6218B36C1D19D4A2E9EB0CE3606EB48.jld2" # Database

# -- Code --
main(inputFile)