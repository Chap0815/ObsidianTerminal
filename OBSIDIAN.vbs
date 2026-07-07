' OBSIDIAN.vbs — Startet Trading Terminal ohne CMD-Fenster
' Nutzt lokales python/pythonw.exe (embedded) wenn vorhanden,
' sonst System-Python als Fallback.

Set WshShell   = CreateObject("WScript.Shell")
Set FSO        = CreateObject("Scripting.FileSystemObject")

strScriptDir   = FSO.GetParentFolderName(WScript.ScriptFullName)

' Python-Pfad bestimmen: lokal zuerst, dann System
strLocalPython = strScriptDir & "\python\pythonw.exe"
If FSO.FileExists(strLocalPython) Then
    strPython = strLocalPython
Else
    strPython = "pythonw"   ' System-Python (muss im PATH sein)
End If

strLauncher = strScriptDir & "\launcher.pyw"

' Launcher starten, kein Fenster (0 = versteckt, False = nicht warten)
WshShell.Run """" & strPython & """ """ & strLauncher & """", 0, False

Set FSO      = Nothing
Set WshShell = Nothing
