function Set-PGEnvVars {
    param(
        $PGDATA,
        $PGHOST="localhost",
        $PGPORT=5433,
        $PGUSER="postgres",
        $PGPASS=$null,
        $PGDATABASE="edmo"
    )

    $Env:PGDATA="${PGDATA}"
    $Env:PGHOST="$PGHOST"
    $Env:PGPORT="$PGPORT"
    $Env:PGUSER="${PGUSER}"
    if ($PGPASS) {
        $Env:PGPASS="${PGPASS}"
    } else {
        $Env:PGPASS=$(python -c "import secrets; print(secrets.token_urlsafe(16))")
        if ($LastExitCode -ne 0) { throw "ERROR: `odmkraken.deploy genpw` failed." }
    }
}


function Get-ServiceBatch {
    param(
        $cmd
    )
    @"
@echo off
call ${Env:CONDA_PREFIX}\condabin\conda.bat activate
$cmd
"@
}


function Install {

    param(
        $DBNAME="edmo",
        $Prefix="C:\srv"
    )

    # set respective paths
    Set-PGEnvVars -PGDATA "${Prefix}\db"
    $Env:DAGSTER_HOME="${Prefix}\dagster"
    $Env:NGINX_HOME="${Prefix}\www"
    $ETC="${Prefix}\etc"
    $LOGS="${Prefix}\logs"

    # ensure nginx, config and log directgories exist
    New-Item -ItemType Directory -Force $ETC, "${ETC}\nginx"
    New-Item -ItemType Directory -Force $LOGS
    New-Item -ItemType Directory -Force $Env:DAGSTER_HOME, $Env:NGINX_HOME

    # variable to collect uninstall instructions
    $Uninstall = @()

    # produce configuration files
    python nginx.py --out "${ETC}\nginx\nginx.conf" `
        --logdir "${LOGS}" `
        --rootdir "${Env:NGINX_HOME}" `
        --proxy /etl http://localhost:3000 `
        --static / $Env:NGINX_HOME
    $Uninstall += "Remove-Item ${ETC}\nginx\nginx.conf"
    
    # create the database folder
    Write-Output "Initializing Postgres ..."
    $PG_PW_FILE = New-TemporaryFile
    Write-Output $Env:PGPASS | Out-File -FilePath $PG_PW_FILE
    initdb `
        --username="${Env:PGUSER}" `
        --pwfile="${PG_PW_FILE}" `
        --auth=trust `
        --no-instructions
    Remove-Item $PG_PW_FILE
    
    # register it a service, so we may start the DB and use it
    Write-Output "Registering database as Windows service and launching it ..."
    $SRV_PG="ODM: Database (Postgres)"
    pg_ctl register -N "${SRV_PG}" -S auto -o "-p $($Env:PGPORT)"
    Start-Service -Name "${SRV_PG}"
    pg_isready

    $Uninstall += "Stop-Service -Force -Name `"${SRV_PG}`""
    $Uninstall += "pg_ctl unregister -N `"${SRV_PG}`""

    # create the actual odmkraken database in the newly created server
    Write-Output "Creating odmkraken database ..."
    python -m odmkraken.deploy setup --dbname "${DBNAME}"
    if ($LastExitCode -ne 0) { 
        Write-Output "ERROR: database creation failed; removing services and database ..."
        $Uninstall | Out-File -FilePath "${Env:CONDA_PREFIX}\uninstall-odm-services.ps1"
        Remove
        throw "ERROR: `odmkraken.deploy setup` failed." 
    }
    
    # prepare the access tokens
    $DSN=@{
        EDMO_AOO="$(python -m odmkraken.deploy resetpw "${DBNAME}_aoo" --dsn)/${DBNAME}"
        EDMO_RO="$(python -m odmkraken.deploy resetpw "${DBNAME}_aoo" --dsn)/${DBNAME}"
        ODMVP_PROD="postgres://odmvp_aoo@pg-prod-int-1.ctie.etat.lu,pg-prod-int-2.ctie.etat.lu,pg-prod-int-3.ctie.etat.lu/odmvp?sslcert=C:\srv\secrets\prod_odmvp_aoo.cert&sslkey=C:\srv\secrets\prod_odmvp_aoo.key&sslrootcert=C:\srv\secrets\ctie-postgres-root.pem&sslmode=verify-ca`""
        ODMVP_TEST="postgres://odmvp_aoo@pg-test-int-1.ctie.etat.lu,pg-test-int-2.ctie.etat.lu,pg-test-int-3.ctie.etat.lu/odmvp?sslcert=C:\srv\secrets\test_odmvp_aoo.cert&sslkey=C:\srv\secrets\test_odmvp_aoo.key&sslrootcert=C:\srv\secrets\ctie-postgres-root.pem&sslmode=verify-ca`""
    }   
    foreach($key in $($DSN.Keys)) {
        $DSN[$key] = "DSN_$($key)=`"$($DSN[$key])`""
    }

    $DAGSTER_VARS="DAGSTER_HOME=`"${Env:DAGSTER_HOME}`""
    $SCRIPTS="${Env:CONDA_PREFIX}\Scripts"
    
    Write-Output 'Registering orchestrator frontend as Windws service ...'
    $SRV_DAGIT="ODM: Orchestrator Frontend (Dagit)"
    #$dagit_service_bat = "${Env:CONDA_PREFIX}\Scripts\dagit_service.bat"
    #Get-ServiceBatch "python -m dagit -l /etl" | Out-File $dagit_service_bat -Encoding oem
    Create-NSSMService -Name "$SRV_DAGIT" `
        -Description "Provides the ETL system the web-interface to control it" `
        -Executable "${SCRIPTS}\dagit.exe" `
        -Parameters "-l /etl -m odmkraken" `
        -AppDirectory $Env:DAGSTER_HOME `
        -EnvVars "${DAGSTER_VARS}`n$($DSN.EDMO_AOO)`n$($DSN.ODMVP_TEST)`n$($DSN.ODMVP_PROD)" `
        -DependsOn $SRV_PG `
        -ErrorLog "${LOGS}\dagit.errors.log" `
        -OutputLog "${LOGS}\dagit.output.log"
    Start-Service $SRV_DAGIT
    $Uninstall += "Stop-Service -Force -Name `"${SRV_DAGIT}`""
    $Uninstall += "nssm remove `"${SRV_DAGIT}`" confirm"


    Write-Output 'Registering orchestrator daemon as Windows service ...'
    $SRV_DAGSTER="ODM: Scheduler (Dagster Daemon)"
    #$dagster_daemon_bat = "${Env:CONDA_PREFIX}\Scripts\dagster_daemon.bat"
    #Get-ServiceBatch "dagster-daemon.exe run" | Out-File $dagster_daemon_bat -Encoding oem
    Create-NSSMService -Name $SRV_DAGSTER `
        -Description "Triggers Dagit-jobs upon certain events or on a schedule." `
        -Executable "${SCRIPTS}\dagster-daemon.exe" `
        -Parameters "run -m odmkraken" `
        -AppDirectory $Env:DAGSTER_HOME `
        -EnvVars $DAGSTER_VARS `
        -DependsOn $SRV_PG `
        -ErrorLog "${LOGS}\dagster-daemon.errors.log" `
        -OutputLog "${LOGS}\dagster-daemon.output.log"

    Start-Service $SRV_DAGSTER
    $Uninstall += "Stop-Service -Force -Name `"${SRV_DAGSTER}`""
    $Uninstall += "nssm remove `"${SRV_DAGSTER}`" confirm"

    Write-Output 'Registering NGINX as Windows service ...'
    $SRV_NGINX="ODM: Webserver (NGINX)"
    Create-NSSMService -Name $SRV_NGINX `
        -Description "Reverse proxy shielding web applications." `
        -Executable "${Env:CONDA_PREFIX}\Library\bin\nginx.exe" `
        -Parameters "-c ${ETC}\nginx\nginx.conf -e ${LOGS}\nginx.error.log" `
        -AppDirectory $Env:TEMP `
        -DependsOn $SRV_DAGIT
    Start-Service $SRV_NGINX
    $Uninstall += "Stop-Service -Force -Name `"${SRV_NGINX}`""
    $Uninstall += "nssm remove `"${SRV_NGINX}`" confirm"
    $Uninstall += "remove-item -Recurse -Force ${Env:PGDATA}"

    # create uninstall script
    $Uninstall | Out-File -FilePath "${Env:CONDA_PREFIX}\uninstall-odmkraken-services.ps1"

    Set-Clipboard -Value "$Env:PGPASS"
    Write-Output "Database superuser password has been copied to clipboard."

}

function Create-NSSMService {
    param(
        $Name,
        $Executable,
        $Description=$null,
        $Parameters=$null,
        $DependsOn=$null,
        $AppDirectory=$null,
        $EnvVars=$null,
        $ErrorLog=$null,
        $OutputLog=$null
    )
    nssm install "${Name}" "${Executable}"
    if ($Parameters) { nssm set "${Name}" AppParameters "${Parameters}" }
    if ($Description) { nssm set "${Name}" Description "${Description}" }
    if ($DependsOn) { nssm set "${Name}" DependOnService "${DependsOn}" }
    if ($AppDirectory) { nssm set "${Name}" AppDirectory "${AppDirectory}" }
    if ($EnvVars) { nssm set "${Name}" AppEnvironmentExtra "${EnvVars}" }
    if ($ErrorLog) { nssm set "${Name}" AppStderr "${ErrorLog}" }
    if ($OutputLog) { nssm set "${Name}" AppStdout "${OutputLog}" }
}

function Remove {
    $uninstall_services="${Env:CONDA_PREFIX}\uninstall-odmkraken-services.ps1"
    . $uninstall_services
    remove-item $uninstall_services
}


conda deactivate
conda activate base

Remove
Install
