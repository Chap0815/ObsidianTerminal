' OBSIDIAN.vbs - startet das Trading Terminal ohne CMD-Fenster.
' Bevorzugt das lokale .venv, danach portable python/, danach System-Python.

Set WshShell = CreateObject("WScript.Shell")
Set FSO = CreateObject("Scripting.FileSystemObject")

strScriptDir = FSO.GetParentFolderName(WScript.ScriptFullName)
strVenvPython = strScriptDir & "\.venv\Scripts\pythonw.exe"
strLocalPython = strScriptDir & "\python\pythonw.exe"

If FSO.FileExists(strVenvPython) Then
    strPython = strVenvPython
ElseIf FSO.FileExists(strLocalPython) Then
    strPython = strLocalPython
Else
    strPython = "pythonw"
End If

strLauncher = strScriptDir & "\launcher.pyw"
WshShell.Run """" & strPython & """ """ & strLauncher & """", 0, False

Set FSO = Nothing
Set WshShell = Nothing
