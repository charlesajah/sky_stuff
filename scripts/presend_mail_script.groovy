// Read the console log as text
def consoleLog = build.log

// Define the regex patterns
def pattern1 = /_datapoint:DatabaseAvailability:([^:]+):status:0/
def pattern2 = /freepct:(\d+\.\d+)/

// Extract lines matching the first pattern
def matches1 = consoleLog.readLines().findAll { line -> line =~ pattern1 }

// Extract lines matching the second pattern
def matches2 = consoleLog.readLines().findAll { line -> line =~ pattern2 }

// Create the email content
def emailContent = "Database Availability Status: 0\n\n"
emailContent += matches1.join("\n")

emailContent += "\n\nTablespace Free Percentage:\n\n"
emailContent += matches2.join("\n")


// Set the email content
msg.setContent(emailContent, "text/plain")