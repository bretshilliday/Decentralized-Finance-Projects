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

# Loading the database (Uncomment the active one)
uniswapTransfers = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Increase.jld2")
uniswapDecreases = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Decrease.jld2")
# uniswapTransfers = load_object("/home/bret.shilliday/FNCE559.09/Homework/UniswapV3Trans.jld2")


# Get the unique chains from the data and print them to terminal
increaseChains = unique(uniswapTransfers.chain_name)
println(increaseChains)
decreaseChains = unique(uniswapDecreases.chain_name)
print(decreaseChains)

# Plot the count of the transfer transactions on each chain
uniswapTransfers |> @vlplot(:bar,x="chain_name",y="count()")

# Plot the count of the decrease liquity transactions on each chain
uniswapDecreases |> @vlplot(:bar,x="chain_name",y="count()")

# Convert the value in Data0 to a BigInt from a hexidecimal string (Note: This could be done in one line)
tempLiquidityAmount = tryparse.(BigInt,uniswapTransfers.data0,base=16)
uniswapTransfers.data0 = tempLiquidityAmount

# Gets the number of transfers per topic3 which is the NFT ID
data_group1 = groupby(uniswapTransfers, :topic3)
sum_of_nft_transfers = combine(data_group1, nrow)
delete!(sum_of_nft_transfers, 1)
rename!(sum_of_nft_transfers,:nrow => :num_transfers)
describe(sum_of_nft_transfers)

# Gets the number of decrease in liquidity per topic3 which is the NFT ID
data_group2 = groupby(uniswapDecreases,:topic1)
sum_of_nft_decreases = combine(data_group2, nrow)
rename!(sum_of_nft_decreases,:nrow => :decreases)
printData(sum_of_nft_decreases, 10)

# Format into a single dataframe where the NFT ID has both the number of transfers and decreases of lquidity
final = innerjoin(sum_of_nft_transfers, sum_of_nft_decreases, on = :topic1)
describe(final)



