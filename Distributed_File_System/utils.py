import os


def get_hostname_from_proc_id(proc_id):
    "retuen hostname from proc_id"
    return proc_id.split(":")[0]


def get_host_number_from_hostname(hostname):
    "return host number from hostname"
    return int(hostname[-2:])


def get_hostname_from_host_num(host_num):
    "return hostname from host number"
    return "fa22-cs425-19" + str(host_num).zfill(2)


def get_proc_id_from_hostname(hostname, membership_list):
    "return proc_id from hostname"
    for member in membership_list:
        if hostname in member:
            return member


def get_port_from_hostname(hostname):
    "return port from hostname"
    return 6000 + int(hostname[-2:])


def adjust_file_name(file_name, REP=False, DEL=False):
    "Adjust file name to match buffer size"
    size = 42
    if REP:
        size -= 1
    elif DEL:
        size -= 1
    if len(file_name) > size:
        return file_name[:size]
    else:
        return file_name.ljust(size, " ")


def get_lastet_version(file_name):
    "return latest version of file_name"
    files = os.listdir("sdfs_files/")
    files = [file for file in files if file.startswith(file_name)]
    if len(files) == 0:
        return file_name
    files.sort()
    return files[-1]


def get_all_versions(file_name):
    "return all versions of file_name"
    files = os.listdir("sdfs_files/")
    files = [file for file in files if file.startswith(file_name)]
    if len(files) == 0:
        return file_name
    files.sort()
    return files


def get_all_versions_from_metadata(file_name, metadata):
    "return all versions of file_name"
    versions = set()
    for proc_id in metadata:
        for meta_file_name in metadata[proc_id]:
            if meta_file_name.startswith(file_name):
                versions.add(meta_file_name)
    return sorted(list(versions))


def get_file_name_ext_version(file_name):
    "return file_name, ext, version"
    ind = file_name.rfind(".")
    file_name, extension, version = file_name[:ind], file_name[ind:], 0
    return file_name, extension, version


def get_file_name_from_sdfs_file_name(sdfs_file_name):
    "return file_name, ext, version"
    ind = sdfs_file_name.rfind(".")
    file_name = sdfs_file_name[: ind - 3] + sdfs_file_name[ind:]
    return file_name


def check_if_file_exists(file_name, is_uploader=False):
    "check if file exists in sdfs_files"
    files = os.listdir("sdfs_files/")
    if is_uploader:
        files = os.listdir("./")
    files = [file for file in files if file.startswith(file_name)]
    if len(files) == 0:
        return False
    return True


def get_latest_version(file_name, is_downloader=False, is_replicator=False):
    "return latest version of file_name"
    files = os.listdir("sdfs_files/")
    if is_downloader:
        files = os.listdir("downloads/")
    if is_replicator:
        files = os.listdir("sdfs_files/")
    files = [file for file in files if file.startswith(file_name)]
    if len(files) == 0:
        return file_name
    files.sort()
    return files[-1]


def get_proc_ids_of_sdfs_file(file_name, metadata):
    "return proc_ids of sdfs_file"
    proc_ids = set()
    for proc_id in metadata:
        for meta_file_name in metadata[proc_id]:
            if meta_file_name.startswith(file_name):
                proc_ids.add(proc_id)
    return proc_ids


def store(metadata, proc_id):
    "store metadata in proc_id"
    files = list(metadata[proc_id])
    return files


def check_if_file_exists_in_metadata(file_name, metadata):
    "check if file exists in metadata"
    normal_file_name, _, _ = get_file_name_ext_version(file_name)
    for proc_id in list(metadata.keys()):
        for meta_file_name in list(metadata[proc_id]):
            if meta_file_name.startswith(normal_file_name):
                return True
    return False

