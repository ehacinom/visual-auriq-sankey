def repeatcmd(name, var1, val1, var2, val2):
    '''Return string value'''
    cmd = 'SELECT COUNT(*) FROM %s WHERE conversion = 0 and %s = %s'
    cmd = cmd % (name, var1, val1, var2, val2)
    cur.execute(cmd)
    return str(cur.fetchone()[0])