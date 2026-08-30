' OBSIDIAN.vbs - startet den crash-sicheren UI-Supervisor ohne CMD-Fenster.
' Bevorzugt das offizielle private python/, danach .venv, dann System-Python.

Set WshShell = CreateObject("WScript.Shell")
Set FSO = CreateObject("Scripting.FileSystemObject")
Set ProcessEnv = WshShell.Environment("PROCESS")
ProcessEnv("PYTHONPATH") = ""
ProcessEnv("PYTHONHOME") = ""
ProcessEnv("PYTHONNOUSERSITE") = "1"

strScriptDir = FSO.GetParentFolderName(WScript.ScriptFullName)
strVenvPython = strScriptDir & "\.venv\Scripts\pythonw.exe"
strLocalPython = strScriptDir & "\python\pythonw.exe"

If FSO.FileExists(strLocalPython) Then
    strPython = strLocalPython
ElseIf FSO.FileExists(strVenvPython) Then
    strPython = strVenvPython
Else
    strPython = "pythonw"
End If

WshShell.CurrentDirectory = strScriptDir
WshShell.Run """" & strPython & """ -m launcher.supervisor", 0, False

Set FSO = Nothing
Set ProcessEnv = Nothing
Set WshShell = Nothing
