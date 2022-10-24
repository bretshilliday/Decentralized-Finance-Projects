import Pkg

# Loading the minimum packages to run the example
using DataFrames
using CSV
using VegaLite
using Dates
using JLD2

print("Loading Objects")

#loading in the database
myDatabase = load_object("/data/home/DefiClass2022/databases/database1.jld2")

#eiminating records without a topic 0 
myDatabase = myDatabase[.!(isnothing.(myDatabase.topic0)),:]

# Function to standardize hashes --> Citing Jamie's Tutorial 
function topic_standard(target_topic)
    uppercase(target_topic)[(end-63):end]
end

# As the database topic0 strings are in all caps and there is no 0x denoting a hex, we want to first standardize the topics before looking at them in the database
topic_hash_univ3_transfer = topic_standard("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
# topic_hash_univ3_increase = topic_standard("0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f")
# topic_hash_univ3_decrease = topic_standard("0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4")
# topic_hash_transfer = topic_standard("0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f")
# topic_hash_univ2_mint = topic_standard("0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f")
# topic_hash_univ2_burn = topic_standard("0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496")
# topic_hash_curve_add = topic_standard("0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768")
# topic_hash_curve_remove = topic_standard("0x5ad056f2e28a8cec232015406b843668c1e36cda598127ec3b8c59b8c72773a0")

# Get records with only uniswap v3 topic transfer 
uniswapV3transfer = myDatabase[myDatabase.topic0 .== topic_hash_univ3_transfer,:]
save_object("UniswapV3Transfer.jld2", uniswapV3transfer)

# Get records with only uniswap v3 topic increase liquidity 
# uniswapV3increase = myDatabase[myDatabase.topic0 .== topic_hash_univ3_increase,:]
# save_object("UniswapV3Increase.jld2", uniswapV3increase)

# Get records with only uniswap v3 topic decrease liquidity 
# uniswapV3decrease = myDatabase[myDatabase.topic0 .== topic_hash_univ3_decrease,:]
# save_object("UniswapV3Decrease.jld2", uniswapV3decrease)

# Get records with only uniswap v2 topic mint
# uniswapV2mint = myDatabase[myDatabase.topic0 .== topic_hash_univ2_mint,:]
# save_object("UniswapV2Mint.jld2", uniswapV2mint)

# Get records with only uniswap v2 topic burn
# uniswapV2burn = myDatabase[myDatabase.topic0 .== topic_hash_univ2_burn,:]
# save_object("UniswapV2Burn.jld2", uniswapV2burn)

# Get records with only curve.fi topic add liquidity 
# curveAdd = myDatabase[myDatabase.topic0 .== topic_hash_curve_add,:]
# save_object("CurveAdd.jld2", curveAdd)

# Get records with only curve.fi topic remove liquidity 
# curveRemove = myDatabase[myDatabase.topic0 .== topic_hash_curve_remove,:]
# save_object("CurveRemove.jld2", curveRemove)

# Get records with only transfer LP NFT
# trans = myDatabase[myDatabase.topic0 .== topic_hash_transfer,:]
# save_object("transfer.jld2", trans)
