function Get-DecodedString { 
    [cmdletbinding()]Param (
    [parameter(ValueFromPipeline)]$wrong_string
    )
    
    #$wrong_string = $wrong_string | Out-String
    $utf8 = [System.Text.Encoding]::GetEncoding(65001) 
    $iso88591 = [System.Text.Encoding]::GetEncoding(1254) #ISO 8859-1 ,Latin-1

    $wrong_bytes = $utf8.GetBytes($wrong_string)

    $right_bytes = [System.Text.Encoding]::Convert($utf8,$iso88591,$wrong_bytes) #Look carefully 
    $right_string = $utf8.GetString($right_bytes) #Look carefully 
    $right_string
}

function Get-CouchbaseData {
    [CmdletBinding()] param (
        [string]$url,
        [string]$SqlInstance,
        [string]$Database,
        [string]$Table,
        [string]$selectQuery,
        [string]$errorMailFrom,
        [string]$errorMailTo,
        [string]$emailDomain,
        [string]$apiKey,
        $base64AuthInfo
    )
    try {
    
        $ApiResult = Invoke-RestMethod $url+$selectQuery -Headers @{Authorization = ("Basic {0}" -f $base64AuthInfo) } 
        
        if ($ApiResult.status -eq "success") {
            if (-not ([string]::IsNullOrEmpty($ApiResult.results))){
                $Json = $ApiResult.results | ConvertTo-Json -Depth 10 | Out-String | Get-DecodedString
                Invoke-DbaQuery -SqlInstance $SqlInstance -Database $Database -Query $insertQuery -SqlParameters @{ Json = $Json } -As SingleValue -EnableException
            }
            else {
                Write-Output "No available data."
            }   
        }
        else {
            throw "Failed: Invoke-RestMethod"
        }

    }
    catch {
        $fnName = $MyInvocation.MyCommand.Name
        $ErrorMsg = $fnName + " - " + ($_ | fl | Out-String)
        Send-MailgunEmail -from "SQL Server <" + $errorMailFrom + ">" -to $errorMailTo -subject "Archiving ($fnName) Failed (PowerShell)" -body $ErrorMsg -emaildomain $emailDomain -apikey $apiKey
        throw;
    }
}

function Remove-CouchbaseData {
    [CmdletBinding()] param (
        [string]$url,
        [string]$SqlInstance,
        [string]$Database,
        [string]$Table,
        [int]$Throttle,
        [int]$Retry,
        [string]$ArchiveDataQuery,
        [string]$deleteCBQuery,
        [string]$deletionSucceeds,
        [string]$deletionFails,
        [string]$errorMailFrom,
        [string]$errorMailTo,
        [string]$emailDomain,
        [string]$apiKey,
        $base64AuthInfo
    )
    
    try {

        $ScriptBlock = { Param($SqlInstance, $Database, $Table, $url, $base64AuthInfo, $ArchiveDataQuery)

            while ($true) {
                $ArchiveData = Invoke-DbaQuery -SqlInstance $SqlInstance -Database $Database -Query $ArchiveDataQuery -EnableException
                if (($ArchiveData | Measure-Object).Count -eq 0) { break; }
    
                foreach ($row in $ArchiveData) {
            
                    $Id = $row.Id
                    
                    $deleteQuery = ($deleteCBQuery -f $Id)

                    [int]$retry_delete = $Retry
                    do {
                        if ($retry_delete -ne $Retry) { Start-Sleep -Milliseconds 50 }
                        $IsDeleted = Invoke-RestMethod $url+$deleteCouponQuery -Headers @{Authorization = ("Basic {0}" -f $base64AuthInfo) } -Method Post
                        $retry_delete -= 1      
                    } until ($IsDeleted.status -eq "success" -or $retry_delete -eq 0)
    
                    if ($IsDeleted.status -eq "success") {
                        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $Database -Query ($deletionSucceeds -f $Id) -EnableException    
                    }
                    else {
                        Invoke-DbaQuery -SqlInstance $SqlInstance -Database $Database -Query ($deletionFails -f $Id) -EnableException   
                    }
                }
            
            }
    
        }

    }
    catch {
        $fnName = $MyInvocation.MyCommand.Name
        $ErrorMsg = $fnName + " - " + ($_ | fl | Out-String)
        Send-MailgunEmail -from "SQL Server <" + $errorMailFrom + ">" -to $errorMailTo -subject "Couchbase Archiving ($fnName) Failed (PowerShell)" -body $ErrorMsg -emaildomain $emailDomain -apikey $apiKey
        throw;
    }


}

//-----PARAMETERS STARTS-----

//Select query. In this example, get the messages older than a week and marked as Archive
$selectQuery = "SELECT META().id, * FROM Messages WHERE mDate < DATE_ADD_STR(NOW_UTC(), -7, 'day') AND state == 'ARCHIVE' LIMIT 2000;"
//Source Couchbase Rest Api url
$url = "http://tt-xxxx-yyyy25.example.com:8093/query/service?statement="
//Source Couchbase Credentials
$username = "Administrator"
$password = "123456"
$base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $username, $password)))
//Target SQL Server Name
$SqlInstance = "xyz.live.example.com"
//Target SQL Server Database 
$Database = "Message_Archive"
//Target SQL Server Database Table
$Table = "Messages"
//Error mail From Address
$errorMailFrom = "from@example.com"
//Error mail To Address
$errorMailTo = "database_team@example.com"
//EMail server
$emailDomain = "examplemail.com"
$apiKey = "key-8ab7f092d7e406d62d28aae5956ce851"
//Where your select query moves and gets inserted into SQL DB. In this case it is a stored procedure that receives data as Json
$insertQuery = "EXEC dbo.InsertCouponJson @Json = @Json"
//Where your records that have been moved get marked as DONE (Status = 1) and also returned as a list to be removed from the Couchbase db
$ArchiveDataQuery = "UPDATE TOP (100) Messages SET Status = 1 OUTPUT JSON_VALUE(INSERTED.Message,'$.id') AS Id WHERE Status = 0"
//Where your data get deleted from Couchbase
$deleteCBQuery = "DELETE FROM Message WHERE meta(Message).id = 'Message:{0}'"
//Mark your records in SQL as successful
$deletionSucceeds = "UPDATE $Table SET Status = 2 WHERE id='Message:{0}'"
//Mark your records in SQL as failed
$deletionFails = "UPDATE $Table SET Status = 3 WHERE id='Message:{0}'"

$retry = 15
$Throttle = 20

//-----PARAMETERS ENDS-----

Import-Module PoshRSJob
Import-Module dbatools

Get-CouchbaseData    -url $url -SqlInstance $SqlInstance -Database $Database -Table $Table -base64AuthInfo $base64AuthInfo 
Remove-CouchbaseData -url $url -SqlInstance $SqlInstance -Database $Database -Table $Table -base64AuthInfo $base64AuthInfo -Throttle $Throttle -Retry $Retry  