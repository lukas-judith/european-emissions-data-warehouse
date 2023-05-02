import pickle

def find_optimal_number_of_AZs(num_subnets, num_azs):
    """
    Given an even number of subnets to be distributed across multiple AZs,
    determine the number of AZs to use. Follow this rule:

    1. Use as many AZs as possible, while using at least two subnets per AZ.
    2. If creating less than 4 subnets, use 2 AZs.
    """
    if num_subnets >= 4:
        return min(num_azs, num_subnets // 2)
    else:
        return 2
    

def save_object(obj, path):
    """
    Save any Python object to a local file using pickle.
    """
    with open(path, 'wb') as file:
        pickle.dump(obj, file)


def load_object(path):
    """
    Load any object that has been saved using pickle.
    """
    with open(path, 'rb') as file:
        obj = pickle.load(file)
    return obj