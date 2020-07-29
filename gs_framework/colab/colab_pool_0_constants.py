import faust.types


class Configuration:

    pool_name = "colab_env_pool_0"

    pool_env_status_topic = faust.types.TP(topic="colab_pool_env", partition=1)
    pool_env_rpc_callee_topic = pool_env_status_topic

    pool_client_rpc_caller_topic = faust.types.TP(topic="colab_pool_client_rpc_caller", partition=1)

    init_google_accounts = [...] # your google accounts list
