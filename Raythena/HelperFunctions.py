import os

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

def getSetupCommand(asetup, extra_setup_commands, run_dir):
    command = """mkdir -p %s; \
                    cd %s; \
                    source /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/user/atlasLocalSetup.sh; \
                    asetup %s; \
                    echo "asetup done!"; \
                    """ % (run_dir, run_dir, asetup)
    
    for c in extra_setup_commands:
        command += """%s;
        """ % c
    return command
