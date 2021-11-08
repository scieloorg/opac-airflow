# Disponibiliza as bases title e issue em XC_KERNEL_GATE

if [ -f SyncToKernel.ini ];
then
    . ./SyncToKernel.ini
fi

if [ "" != "${XC_KERNEL_GATE}" ] && [ -e ${XC_KERNEL_GATE} ];
then
    if [ -f ../serial/title/title.mst ];
    then
        cp ../serial/title/title.mst $XC_KERNEL_GATE
        cp ../serial/title/title.xrf $XC_KERNEL_GATE
    fi
    if [ -f ../serial/issue/issue.mst ];
    then
        cp ../serial/issue/issue.mst $XC_KERNEL_GATE
        cp ../serial/issue/issue.xrf $XC_KERNEL_GATE
    fi
fi
