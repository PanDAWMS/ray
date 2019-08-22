import os
import stat

def getParameter(config, options, parameter):
    configPar = getattr(config, parameter)
    optionsPar = getattr(options, parameter)
    print ("%s %s %s" % (parameter, configPar, optionsPar))

    # convert to int if necessary
    if optionsPar:
        integerConfig = True
        for s in optionsPar:
            if s not in [str(x) for x in range(10)]:
                integerConfig = False
                break
        if integerConfig:
            optionsPar = int(optionsPar)

    if optionsPar:
        return optionsPar
    else:
        return configPar

def getUniqueDir(path):
    n = 0
    outPath = path
    while(os.path.isdir(outPath)):
        n += 1
        outPath = path + "_%s" % n
    return outPath

def get_setup_command(asetup, extra_setup_commands, run_dir):
    command = """cd %s; \
                 source /global/project/projectdirs/atlas/scripts/setupATLAS.sh; \
                 setupATLAS; \
                 asetup %s; \
                 echo "asetup done!";""" % (run_dir, asetup)
    
    for c in extra_setup_commands:
        command += """%s;
        """ % c
    return command

def make_run_dir_and_script(run_dir, command, script_name):
    # make the run directory
    if not os.path.exists(run_dir):
        os.makedirs(run_dir)

    # make a shell script with the command
    run_script = os.path.join(run_dir, script_name)
    with open(run_script, 'w+') as f:
        f.write(command)
    st = os.stat(run_script)
    os.chmod(run_script, st.st_mode | stat.S_IEXEC)
    return run_script

