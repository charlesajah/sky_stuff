// Import necessary Java classes for regex
import java.util.regex.Pattern
import java.util.regex.Matcher

// Read the Jenkins console log as text
def consoleLog = build.log

// Remove lines after 'Searching console output'
def filteredConsoleLog = consoleLog.readLines().takeWhile { line -> !(line.contains("[Text Finder] Searching console output")) }

// Join the filtered lines to create a single text block
def filteredConsoleText = filteredConsoleLog.join('\n')

// Initialize a list to capture debug messages
def debugMessages = []

// Define the regex pattern for pattern1
def pattern1 = /Could not connect to the PostgreSQL instance on ([^ \.]+)/

// Extract lines matching pattern1
def matches1 = filteredConsoleText =~ pattern1

// Updated regex pattern to allow for hidden spaces or formatting issues
//def pattern2 = /write_lag on ([\w-]+)\s*is:\s*(\d+)[\s\S]*?flush_lag on \1\s*is:\s*(\d+)[\s\S]*?replay_lag on standby instance\s*is:\s*(\d+)\./
def pattern2 = /write_lag on ([\w-]+)\s*is:\s*(\d+:\d+:\d+)\. flush_lag on \1\s*is:\s*(\d+:\d+:\d+)\. replay_lag on standby instance\s*is:\s*(\d+:\d+:\d+)\./

// Extract lines matching pattern2
def matches2 = filteredConsoleText =~ pattern2

// Define the regex pattern for trapping table locks
//def patternLockWait = /The PID (\d+) on database (\w+) is waiting for a lock in [\w]+ mode being held by pid (\d+) since (\d+:\d+:\d+)\./
// Define the updated regex pattern for capturing host, database, blocker PID, blocked PID, and duration
def patternLockWait = /The PID (\d+) on database (\w+) and host\s+([\w-]+) is waiting for a lock in ([\w]+) mode being held by pid (\d+) since (\d+:\d+:\d+)\./



// Extract lines matching the patternLockWait
def matchesLockWait = filteredConsoleText =~ patternLockWait

// Initialize lists and maps to store extracted data
def pattern1List = []
def pattern2List = []
def lockWaitList = []

// Extract DB Availability status
matches1.each { match -> pattern1List.add(match[1]) }

// Iterate over all matches for pattern2 and extract the lag values if any match is found
matches2.each { match -> 
    def host_name = match[1] // Capture the first matched group in the regex which is host_name
    def write_lag = match[2] // Capture the first group (write_lag)
    def flush_lag = match[3] // Capture the second group (flush_lag)
    def replay_lag = match[4] // Capture the third group (replay_lag)

    // Add the extracted values as a map to the pattern2List
    pattern2List.add([host: host_name, write_lag: write_lag, flush_lag: flush_lag, replay_lag: replay_lag])

    // Capture debug info
    debugMessages.add("Found match: Host=${host_name}, Write Lag=${write_lag}, Flush Lag=${flush_lag}, Replay Lag=${replay_lag}")
}

// Iterate over all matches for patternLockWait and extract the necessary values
matchesLockWait.each { match ->
    def blocker = match[1] // PID of the process waiting for the lock
    def database = match[2] // Name of the database
    def host = match[3] //hostname
    def lock = match[4] // lock type
    def blocked = match[5] // PID of the process holding the lock
    def duration = match[6] // Duration the lock has been held

    // Add the extracted values as a map to the lockWaitList
    lockWaitList.add([lock: lock, host: host, database: database, blocker: blocker, blocked: blocked, duration: duration])

    // Capture debug info
    debugMessages.add("Found match: Database=${host}, ${database}, Blocker PID=${blocker}, Blocked PID=${blocked}, Duration=${duration}")
}


// Build the email content
def emailContent = """
<!DOCTYPE html>
<html>
<head>
<style>
  table {
    border-collapse: collapse;
    width: 100%;
  }
  th, td {
    border: 1px solid black;
    padding: 8px;
    text-align: left;
  }
</style>
</head>
<body>

<h2>Database Availability Status</h2>
<table>
  <tr>
    <th>Host</th>
    <th>Status</th>
  </tr>
"""

// Populate the Database Availability Status table
for (int i = 0; i < pattern1List.size(); i++) {
    emailContent += "<tr><td>${pattern1List[i]}</td><td>Not Reachable</td></tr>"
}

emailContent += """
</table>

<h2>Repmgr Streaming Status</h2>
<table>
  <tr>
    <th>Primary Instance</th>
    <th>Write Lag</th>
    <th>Flush Lag</th>
    <th>Replay Lag</th>
  </tr>
"""

// Populate the Repmgr Streaming Status table with the extracted data from pattern2List
for (int i = 0; i < pattern2List.size(); i++) {
    def entry = pattern2List[i] // Access each map in the list
    emailContent += "<tr><td>${entry.host}</td><td>${entry.write_lag}</td><td>${entry.flush_lag}</td><td>${entry.replay_lag}</td></tr>"
}

emailContent += """
</table>

<h2>Database Locks</h2>
<table>
  <tr>
    <th>Host</th>
    <th>Database</th>
    <th>Blocker</th>
    <th>Blocked</th>
    <th>LockMode</th>
    <th>Duration(HH24:MI:SS)</th>
  </tr>
"""

// Populate the Lock Wait Status table with the extracted data from lockWaitList
for (int i = 0; i < lockWaitList.size(); i++) {
    def entry = lockWaitList[i] // Access each map in the list
    emailContent += "<tr><td>${entry.host}</td><td>${entry.database}</td><td>${entry.blocker}</td><td>${entry.blocked}</td><td>${entry.lock}</td><td>${entry.duration}</td></tr>"
}

emailContent += """
</table>
"""

// Append debug information to the email content
//emailContent += "<h2>Debug Info:</h2>"
//emailContent += "<ul>"
//debugMessages.each { message ->
  //  emailContent += "<li>${message}</li>"
//}
//emailContent += "</ul>"

// Close the HTML
emailContent += """
</body>
</html>
"""

// Set the email content as HTML
msg.setContent(emailContent + generateConsoleLogLink(), "text/html")

// Function to generate the console log link
def generateConsoleLogLink() {
    def buildUrl = build.absoluteUrl
    return """
    <br />
    <p><a href="${buildUrl}/consoleText">View Console Log</a></p>
    """
}