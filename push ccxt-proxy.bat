chcp 65001
cd %~dp0
set /p c=请输入commit:
git add .
git commit -m "%c%"
git push https://hxse:%hxse_github_token%@github.com/hxse/ccxt-proxy.git
pause
::git reset --hard HEAD^
::git push origin master -f
