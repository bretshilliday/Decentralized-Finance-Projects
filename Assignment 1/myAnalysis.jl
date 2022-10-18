import Pkg
#Pkg.add("JSON")
#Pkg.add("HTTP")
#Pkg.add("DataFrames")
# Pkg.add("CSV")
# Pkg.add("VegaLite")
# Pkg.add("Dates")
# Pkg.add("JLD2")

# Loading the minimum packages to run the example
using DataFrames
using CSV
using VegaLite
using Dates
using JLD2

# Prints the first numRows rows of the dataframe in its entirety to the terminal
function printData(df, numRows)
    show(first(df, numRows), allcols=true, allrows=true, truncate = 0)
    println("\n")
end

# Loading the database
# uniswapTransfers = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Increase.jld2")
# uniswapDecreases = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Decrease.jld2")
uniswapTransfers = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Trans.jld2")


#see the different chains that are involved
# increaseChains = unique(uniswapTransfers.chain_name)
# print(increaseChains)
# print("\n")
# decreaseChains = unique(uniswapDecreases.chain_name)
# print(decreaseChains)

#increaseliquidty chain distrubution 
# uniswapTransfers |> @vlplot(:bar,x="chain_name",y="count()")

# #increaseliquidty chain distrubution 
# uniswapDecreases |> @vlplot(:bar,x="chain_name",y="count()")

# #hex to dec 
# tempLiquidityAmount = tryparse.(BigInt,uniswapTransfers.data0,base=16)
# uniswapTransfers.data0 = tempLiquidityAmount



data_group1 = groupby(uniswapTransfers,:topic3)
sum_of_nft_transfers = combine(data_group1, nrow)

delete!(sum_of_nft_transfers, 1)

rename!(sum_of_nft_transfers,:nrow => :num_transfers)
# printData(sum_of_nft_transfers, 10)

describe(sum_of_nft_transfers)

# data_group2 = groupby(uniswapDecreases,:topic1)
# sum_of_nft_decreases = combine(data_group2, nrow)
# rename!(sum_of_nft_decreases,:nrow => :decreases)
# printData(sum_of_nft_decreases, 10)

# final = innerjoin(sum_of_nft_transfers, sum_of_nft_decreases, on = :topic1)
# printData(final, 10)

# describe(final)



