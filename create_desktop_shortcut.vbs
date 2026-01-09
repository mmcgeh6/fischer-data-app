' Fischer Energy Partners - Desktop Shortcut Creator
' Create a desktop shortcut for the Fischer Data Processing App
'
' Version: 1.0
' Date: 2025-12-19
'
' Purpose:
' --------
' This script creates a desktop shortcut that:
' - Displays the Fischer logo as an icon
' - Launches the app via launch_fischer_app.vbs
' - Shows a helpful description on hover
' - Works with OneDrive-synced desktops
'
' Usage:
' ------
' Run this script ONCE to create the desktop shortcut
' Double-click the created shortcut to launch the app

Option Explicit

Dim objShell, objFSO, objShortcut
Dim strProjectDir, strDesktopPath, strShortcutPath
Dim strVBSLauncher, strIconPath
Dim intResult

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

strVBSLauncher = strProjectDir & "\launch_fischer_app.vbs"
strIconPath = strProjectDir & "\Assets\fischer app logo ico.ico"

' ====================================================================
' Get Desktop Path
' ====================================================================
' This handles both regular desktops and OneDrive-synced desktops

strDesktopPath = objShell.SpecialFolders("Desktop")

' ====================================================================
' Define Shortcut Path
' ====================================================================

strShortcutPath = strDesktopPath & "\Fischer Data Processing App.lnk"

' ====================================================================
' Validation
' ====================================================================
' Check that required files exist

If Not objFSO.FileExists(strVBSLauncher) Then
    MsgBox "Error: VBS launcher not found!" & vbCrLf & vbCrLf & _
           "Expected location:" & vbCrLf & _
           strVBSLauncher & vbCrLf & vbCrLf & _
           "Please ensure launch_fischer_app.vbs exists in the project root.", _
           vbCritical, "Shortcut Creator - Missing Launcher"
    WScript.Quit 1
End If

If Not objFSO.FileExists(strIconPath) Then
    MsgBox "Warning: Icon file not found!" & vbCrLf & vbCrLf & _
           "Expected location:" & vbCrLf & _
           strIconPath & vbCrLf & vbCrLf & _
           "The shortcut will be created with the default icon. " & _
           "You can manually change the icon later.", _
           vbExclamation, "Shortcut Creator - Missing Icon"
End If

' ====================================================================
' Create Shortcut
' ====================================================================
' Instantiate the shortcut object

Set objShortcut = objShell.CreateShortcut(strShortcutPath)

' ====================================================================
' Configure Shortcut Properties
' ====================================================================

With objShortcut
    ' Target: WScript.exe (Windows Script Host interpreter)
    .TargetPath = "WScript.exe"

    ' Arguments: The VBScript launcher path (with proper quoting)
    ' Format: "path\to\launch_fischer_app.vbs"
    .Arguments = """" & strVBSLauncher & """"

    ' Working Directory: Project root
    ' This ensures relative paths work correctly
    .WorkingDirectory = strProjectDir

    ' Description: Shows when hovering over the shortcut
    .Description = "Fischer Energy Partners - Data Processing Application"

    ' Icon Location: Path to .ico file with index
    ' Format: "path\to\file.ico,0" (0 = first icon in file)
    If objFSO.FileExists(strIconPath) Then
        .IconLocation = strIconPath & ",0"
    End If

    ' Window Style: 1 = Normal (VBScript will control actual window style)
    .WindowStyle = 1

    ' Save the shortcut to disk
    .Save
End With

' ====================================================================
' Success Message
' ====================================================================

MsgBox "Desktop shortcut created successfully!" & vbCrLf & vbCrLf & _
       "Location: " & vbCrLf & _
       strShortcutPath & vbCrLf & vbCrLf & _
       "You can now double-click the desktop shortcut " & _
       "'Fischer Data Processing App' to launch the application.", _
       vbInformation, "Shortcut Creator - Success"

' ====================================================================
' Cleanup
' ====================================================================

Set objShortcut = Nothing
Set objFSO = Nothing
Set objShell = Nothing

' Exit successfully
WScript.Quit 0
