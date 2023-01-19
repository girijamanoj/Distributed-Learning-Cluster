def get_hostname_from_proc_id(proc_id):
    return proc_id.split(":")[0]


def get_host_number_from_hostname(hostname):
    return int(hostname[-2:])


def get_hostname_from_host_num(host_num):
    return "fa22-cs425-19" + str(host_num).zfill(2)


def get_proc_id_from_hostname(hostname, membership_list):
    for member in membership_list:
        if hostname in member:
            return member


def get_port_from_hostname(hostname):
    return 6000 + int(hostname[-2:])
