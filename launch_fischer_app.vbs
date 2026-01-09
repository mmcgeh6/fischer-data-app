' Fischer Energy Partners - Data Processing App Launcher
' This VBScript launches the Python launcher with a minimized console window
'
' Version: 1.0
' Date: 2025-12-19
'
' Purpose:
' --------
' Provides a way to launch the Fischer Data Processing App with:
' - Minimized console window (not hidden, visible in taskbar)
' - Professional appearance with no technical jargon
' - Auto-closing error dialogs for troubleshooting
'
' Usage:
' ------
' Double-click this file to launch the app

Option Explicit

Dim objShell, objFSO
Dim strProjectDir, strPythonExe, strLauncherScript
Dim strCommand
Dim intWindowStyle

' ====================================================================
' Initialize COM Objects
' ====================================================================

Set objShell = CreateObject("WScript.Shell")
Set objFSO = CreateObject("Scripting.FileSystemObject")

' ====================================================================
' Get Project Directory
' ====================================================================
' Use the location of this VBScript to find the project root

strProjectDir = objFSO.GetParentFolderName(WScript.ScriptFullName)

' ====================================================================
' Define Paths
' ====================================================================
' Use backslash notation and handle spaces with proper quoting

strPythonExe = strProjectDir & "\.venv311\Scripts\python.exe"
strLauncherScript = strProjectDir & "\launcher.py"

' ====================================================================
' Validation
' ====================================================================
' Check that required files exist before launching

If Not objFSO.FileExists(strPythonExe) Then
    MsgBox "Error: Python executable not found!" & vbCrLf & vbCrLf & _
           "Expected location:" & vbCrLf & _
           strPythonExe & vbCrLf & vbCrLf & _
           "Please ensure the virtual environment (.venv311) is properly set up.", _
           vbCritical, "Fischer Data App - Missing Python"
    WScript.Quit 1
End If

If Not objFSO.FileExists(strLauncherScript) Then
    MsgBox "Error: Launcher script not found!" & vbCrLf & vbCrLf & _
           "Expected location:" & vbCrLf & _
           strLauncherScript & vbCrLf & vbCrLf & _
           "Please ensure launcher.py exists in the project root.", _
           vbCritical, "Fischer Data App - Missing Launcher"
    WScript.Quit 1
End If

' ====================================================================
' Build Command
' ====================================================================
' Construct the command with proper quoting for paths containing spaces
' Format: "python.exe" "launcher.py"

strCommand = """" & strPythonExe & """ """ & strLauncherScript & """"

' ====================================================================
' Window Style Configuration
' ====================================================================
' Window style options:
'   0 = Hidden (no window visible)
'   1 = Normal (standard window)
'   2 = Minimized (minimized to taskbar) <- Our choice
'   3 = Maximized (fullscreen)

intWindowStyle = 2

' ====================================================================
' Launch Application
' ====================================================================
' Run the Python launcher with specified window style
'
' Parameters:
' - strCommand: The command line to execute
' - intWindowStyle: How to display the window
' - False: Don't wait for process to complete (async)

objShell.Run strCommand, intWindowStyle, False

' ====================================================================
' Cleanup
' ====================================================================

Set objFSO = Nothing
Set objShell = Nothing

' Exit VBScript
WScript.Quit 0
