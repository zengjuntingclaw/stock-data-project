@echo off
cd /d "c:\Users\zengj\.qclaw\workspace\stock_data_project"
git add scripts/
git commit -m "feat: schema v5 data warehouse refactoring"
git push origin master
git log --oneline -3
