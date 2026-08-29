' OBSIDIAN.vbs - startet den crash-sicheren UI-Supervisor ohne CMD-Fenster.
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

WshShell.CurrentDirectory = strScriptDir
WshShell.Run """" & strPython & """ -m launcher.supervisor", 0, False

Set FSO = Nothing
Set WshShell = Nothing
