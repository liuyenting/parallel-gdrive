import re

def find_file_id(message, pattern=r'"(.*)", '):
    token = re.search(pattern, message)
    return token.group(1)

if __name__ == '__main__':
    with open('error.log', 'r') as ifd, open('id.txt', 'w') as ofd:
        for line in ifd:
            line = line.strip()
            file_id = find_file_id(line)
            ofd.write("{}\n".format(file_id))