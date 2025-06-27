// Read the Jenkins console log as text
def consoleLog = build.log

// Remove lines after 'Searching console output'
def filteredConsoleLog = consoleLog.readLines().takeWhile { line -> !(line.contains("[Text Finder] Searching console output")) }

// Join the filtered lines to create a single text block
def filteredConsoleText = filteredConsoleLog.join('\n')

// Define the regex pattern for pattern1
def pattern1 = /_datapoint:DatabaseAvailability:([^:]+):status:0/

// Extract lines matching pattern1
def matches1 = filteredConsoleText =~ pattern1

// Define the regex pattern for pattern2, for capturing floating-point numbers for the free percentage
def pattern2 = /_datapoint:DatabaseTablespace:([^:]+):freepct:(\d+(\.\d+)?)/

// Extract lines matching pattern2
def matches2 = filteredConsoleText =~ pattern2

// Define the regex pattern for pattern3, for capturing whole numbers for space allocated
def pattern3 = ~/Successfully allocated (\d+(\.\d+)?) MB/

// Extract lines matching pattern3
def matches3 = filteredConsoleText =~ pattern3

// Debugging: Verify matches
//println "Matches for pattern1: ${matches1.size()}"
//println "Matches for pattern2: ${matches2.size()}"
//println "Matches for pattern3: ${matches3.size()}"

// Create a map to track space added and "Fixed" status for each dbTablespace
def spaceAddedMap = [:]

// Assuming matches2 has matches, process them
if (matches2.size() > 0) {
    matches3.each { match ->
        def spaceAdded = match[1]
        matches2.each { match2 ->
            def dbTablespace = match2[1]
            spaceAddedMap[dbTablespace] = [spaceAdded: spaceAdded, fixed: 'Yes']
        }
    }
} else {
    println("No matches found in matches2. Cannot associate space added with a tablespace.")
}

// Initialize "Fixed" status to "No" for tablespace entries without a pattern3 match
matches2.each { match ->
    def dbTablespace = match[1]
    if (!spaceAddedMap.containsKey(dbTablespace)) {
        spaceAddedMap[dbTablespace] = [spaceAdded: '0', fixed: 'No']
    }
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
    <th>Database</th>
    <th>Status</th>
  </tr>
"""

matches1.each { match ->
    def database = match[1]
    emailContent += """
  <tr>
    <td>${database}</td>
    <td>0</td>
  </tr>
  """
}

emailContent += """
</table>

<h2>Tablespace Free Percentage</h2>
<table>
  <tr>
    <th>DatabaseName_TablespaceName</th>
    <th>Percentage Free</th>
    <th>Fixed</th>
    <th>Space_added(MB)</th>
  </tr>
"""

matches2.each { match ->
    def dbTablespace = match[1]
    def freePct = match[2]
    def spaceAddedInfo = spaceAddedMap.getOrDefault(dbTablespace, [spaceAdded: '0', fixed: 'No'])
    emailContent += """
  <tr>
    <td>${dbTablespace}</td>
    <td>${freePct}</td>
    <td>${spaceAddedInfo.fixed}</td>
    <td>${spaceAddedInfo.spaceAdded}</td>
  </tr>
  """
}

emailContent += """
</table>
</body>
</html>
"""

// Set the email content as HTML
//msg.setContent(emailContent, "text/html")

// Include the console log link
msg.setContent(emailContent + generateConsoleLogLink(), "text/html")

// Function to generate the console log link
def generateConsoleLogLink() {
    def buildUrl = build.absoluteUrl
    return """
    <br />
    <p><a href="${buildUrl}/consoleText">View Console Log</a></p>
    """
}