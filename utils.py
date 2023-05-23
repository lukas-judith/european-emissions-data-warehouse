import os
import pickle
import shutil
import subprocess


def find_optimal_number_of_AZs(num_subnets, num_azs):
    """
    Given an even number of subnets to be distributed across multiple AZs,
    determines the number of AZs to use. Follow this rule:

    1. Use as many AZs as possible, while using at least two subnets per AZ.
    2. If creating less than 4 subnets, use 2 AZs.
    """
    if num_subnets >= 4:
        return min(num_azs, num_subnets // 2)
    else:
        return 2

def save_object(obj, path):
    """
    Saves any Python object to a local file using pickle.
    """
    with open(path, 'wb') as file:
        pickle.dump(obj, file)

def load_object(path):
    """
    Loads any object that has been saved using pickle.
    """
    with open(path, 'rb') as file:
        obj = pickle.load(file)
    return obj

def handle_exceptions(service_type, operation):
    """
    Decorator for handling exceptions while performing 
    different operations on services/functions.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(f"An error occured when trying to {operation} {service_type}:\n{e}")
        return wrapper
    return decorator


def create_deployment_package(script_path, zip_path, dependencies=None,
                              python_version='python3.8', upgrade_pip=True):
    """
    Creates a deployment package consisting of a Python script and its dependencies.
    
    Args:
        script_path (str): The path to the python script with the lambda function handler.
        zip_path (str): The path to the zip file that will be created.
        dependencies (list): A list of Python libraries to include in the package.
    """
    # create a temporary directory
    temp_dir = 'temp_package_dir'
    os.makedirs(temp_dir, exist_ok=True) 
    
    # copy the Python script to the temporary directory
    shutil.copy(script_path, temp_dir)

    # move to the temporary directory
    os.chdir(temp_dir)

    # install dependencies
    if dependencies:
        # suppress the output of any subprocess
        with open(os.devnull, 'w') as devnull:

            # can try to upgrade pip to avoid any problems
            if upgrade_pip:
                print("Upgrading pip")
                try:
                    subprocess.check_call([python_version, '-m', 'pip', 'install', '--upgrade', 'pip'],
                                        stdout=devnull, stderr=devnull)
                except Exception as e:
                    print(f"Could not update pip: {e}")

            # install the Python packages locally and in the temporary directory
            for package in dependencies:
                # start subprocess to install the package
                print(f"Installing dependencies for Lambda function deployment package ({python_version}): {package}")
                try:
                    subprocess.check_call([python_version, '-m', 'pip', 'install', package, '--upgrade', '--target', '.'],
                                        stdout=devnull, stderr=devnull)
                except Exception as e:
                    print(f"Could not install {package}: {e}")

    # create a zip file (i.e., the deployment package)
    if os.path.exists(zip_path):
        os.remove(zip_path)      
    shutil.make_archive(zip_path.replace('.zip', ''), 'zip', '.')

    # move back to the parent directory
    os.chdir('..')

    # move the zip file to the parent directory
    if os.path.exists(zip_path):
            os.remove(zip_path)  
    shutil.move(f"{temp_dir}/{zip_path}", '.')

    # remove the temporary directory
    shutil.rmtree(temp_dir)
