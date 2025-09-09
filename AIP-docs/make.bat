@ECHO OFF
REM 简化的Windows批处理文件用于Sphinx

set SPHINXBUILD=sphinx-build
set SOURCEDIR=docs\source
set BUILDDIR=docs\_build

:html
%SPHINXBUILD% -b html %SOURCEDIR% %BUILDDIR%\html
echo.
echo HTML文档已构建完成。请打开 %BUILDDIR%\html\index.html 查看。
goto :eof

:clean
if exist %BUILDDIR% rmdir /s /q %BUILDDIR%
goto :eof
