import sys
import re

def print_var(var_name, var_val, var_type='float'):
    if isinstance(var_val, list):
        var_name += '[]'
        var_val = '{{ {0} }}'.format(', '.join([str(i) for i in var_val]))

    var_decl = 'const {0} {1} = {2}; \n'.format(var_type, var_name, var_val)
    print(var_decl)

def parse_vec(line: str):
    match = re.match('.*?:(.*)\((\d+)\)', line)
    data = match.group(1)
    data = data.split(',')
    #  data = [float(d) for d in data if len(d)]
    data = [float(d.strip()) for d in data if len(d.strip())]
    data_len = int(match.group(2))

    assert(len(data) == data_len)

    return data, data_len

def parse_contents(contents: str):
    global lines
    lines = contents.split('\n')

    rb = [l for l in lines if l.startswith('bin')]
    rbc = [l for l in lines if l.startswith('rbc')]
    range_str = [l for l in lines if l.startswith('pivot range')][0]

    range_tuple = re.findall('\((.*?)\)', range_str)[0]
    range_tuple = range_tuple.split(' ')
    range_min = range_tuple[0]
    range_max = range_tuple[1]

    rb_vec, rb_vec_len = parse_vec(rb[0])
    rbc_vec, rbc_vec_len = parse_vec(rbc[0])

    print_var('num_ranks', rbc_vec_len, 'int')
    print_var('oob_data', [])
    print_var('oob_data_sz', 0, 'int')
    print_var('range_min', range_min)
    print_var('range_max', range_max)

    print_var('num_pivots', 64, 'int')
    
    print_var('rank_bin_counts', rbc_vec)
    print_var('rank_bins', rb_vec)

    pass

def parse(fpath: str):
    contents = open(fpath).read()
    parse_contents(contents)

if __name__ == '__main__':
    fpath = 'pvtstate.txt'
    parse(fpath)
    pass
