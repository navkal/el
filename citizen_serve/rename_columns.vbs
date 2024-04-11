' Copyright 2024 Energize Lawrence.  All rights reserved.
'

' Initialize console output
Set fso = CreateObject( "Scripting.FileSystemObject" )
Set stdout = fso.GetStandardStream(1)

' Report copyright notice
stdout.WriteLine "Copyright 2024 Energize Lawrence.  All rights reserved."

' Determine current working directory
Dim WshShell, strCurDir
Set WshShell = CreateObject( "WScript.Shell" )
s_dir = WshShell.CurrentDirectory

' Create temporary working copy of file
org_filename = s_dir & "\City of Lawrence Assessment Export.xlsx"
tmp_filename = s_dir & "\CitizenServe Import.xlsx"
If fso.FileExists( tmp_filename ) Then
  fso.DeleteFile( tmp_filename )
End If
fso.CopyFile org_filename, tmp_filename

' Initialize dictionary of column name mappings
Set dc_mappings = CreateObject( "Scripting.Dictionary" )

' Read the column name mappings
Set csvFile = fso.OpenTextFile( s_dir & "\column_name_map.csv" )
Do While csvFile.AtEndOfStream <> True
  s_line = csvFile.ReadLine
  a_mapping = Split( s_line, "," )
  dc_mappings( Trim( a_mapping(0) ) ) = Trim( a_mapping(1) )
Loop

' Open the spreadsheet to be modified
Set xlObj = CreateObject( "Excel.Application" )
Set xlFile = xlObj.WorkBooks.Open( tmp_filename )

' Turn off screen alerts
xlObj.Application.DisplayAlerts = False

s_not_renamed = ""

stdout.WriteLine
stdout.WriteLine "Renaming columns:"

' Read the sheet
With xlFile.ActiveSheet

  ' Delete two-row header
  .Rows( "1:2" ).Delete

  ' Get offset of last used column
  n_last_col = .UsedRange.Columns.Count

  ' Iterate over used columns
  For n_col = 1 To n_last_col

    ' Get column name
    s_col_name = .Cells( 1, n_col ).Value

    ' If we have a mapping for the current column name, rename the column
    If dc_mappings.Exists( s_col_name ) Then
      s_new_name = dc_mappings( s_col_name )
      .Cells( 1, n_col ).Value = s_new_name
      stdout.WriteLine "  '" & s_col_name & "' => '" & s_new_name & "'"
    Else
      ' Save unmapped column name for later reporting
      If s_not_renamed <> "" Then
        s_not_renamed = s_not_renamed & ","
      End If
      s_not_renamed = s_not_renamed & s_col_name
    End If

  Next

End With

' Format date, to be used as suffix in filename
Dim dd, mm, yy, s_date
s_date_time = Now()
dd = Right( "00" & Day( s_date_time ), 2 )
mm = Right( "00" & Month( s_date_time ), 2 )
yy = Year( s_date_time )
s_date = yy & "-" & mm & "-" & dd

' Save CSV result
csv_filename = s_dir & "\CitizenServe Import " & s_date & ".csv"
stdout.WriteLine
stdout.WriteLine "Saving result file '" & csv_filename & "'"
stdout.WriteLine
xlFile.SaveAs csv_filename, 6

' Close and delete temporary copy of Excel file
xlFile.Close False
If fso.FileExists( tmp_filename ) Then
  fso.DeleteFile( tmp_filename )
End If

' Report column names that were not changed
If s_not_renamed <> "" Then
  stdout.WriteLine
  stdout.WriteLine "Columns not renamed:"
  a_not_renamed = Split( s_not_renamed, "," )
  For Each s_name In a_not_renamed
    stdout.WriteLine "  '" & s_name & "'"
  Next
  stdout.WriteLine
End IF

xlObj.Quit
