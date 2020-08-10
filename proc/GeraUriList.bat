# GeraUriList.bat
# Dados ACRON e ISSUE
# gera uma lista com as URI deste dado fascículo
# extraída da base `../bases-work/${ACRON}/${ACRON}`

ACRON=$1
ISSUE=$2

$CISIS_DIR/mx ../bases-work/${ACRON}/${ACRON} \
    btell=0 \
    "bool=I=${ISSUE} or H=${ISSUE}" \
    lw=9000 \
    "proc='a9999{${ACRON}{a9990{${ISSUE}{'" \
    pft=@pft/gera_uri_list.pft \
    now

