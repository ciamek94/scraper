# Raspberry Pi: instalacja i obsługa

Instrukcja zakłada Raspberry Pi z Raspberry Pi OS 64-bit, użytkownika `pi`, adres IP `192.168.1.143` oraz katalog projektu `/home/pi/scraper_olx`. Jeśli używasz innego użytkownika, adresu lub katalogu, odpowiednio zmień je w poleceniach. Polecenia `ssh` i `scp` uruchamiaj na komputerze w PowerShellu; polecenia oznaczone jako Raspberry wykonuj po zalogowaniu przez SSH.

## Przygotowanie Raspberry Pi

1. Zainstaluj Raspberry Pi OS 64-bit (Lite wystarczy) na karcie microSD, uruchom Raspberry i połącz je z tą samą siecią co komputer.
2. Włącz SSH w ustawieniach Raspberry Pi OS albo poleceniem `sudo raspi-config` → `Interface Options` → `SSH`.
3. Ustal adres IP Raspberry w ustawieniach routera lub na Raspberry poleceniem `hostname -I`.
4. W PowerShellu sprawdź połączenie, podając użytkownika i adres Raspberry:

```powershell
ssh pi@192.168.1.143
```

Przy pierwszym połączeniu potwierdź klucz hosta tylko wtedy, gdy adres należy do Twojego Raspberry. Wpisz hasło użytkownika Raspberry. Po sprawdzeniu systemu wyjdź z SSH poleceniem `exit`.

## Sprawdzenie systemu

Playwright wymaga 64-bitowego systemu. Po zalogowaniu przez SSH sprawdź architekturę:

```sh
uname -m
getconf LONG_BIT
dpkg --print-architecture
```

Oczekiwane wyniki: `aarch64`, `64`, `arm64`.

Jeśli architektura to `armv7l` lub system jest 32-bitowy, zainstaluj 64-bitowy Raspberry Pi OS. Playwright/Chromium może nie działać na 32-bitowym systemie.

## Pierwsza instalacja

Polecenia `ssh` i `scp` wykonuj w PowerShellu na komputerze, w katalogu projektu:

```powershell
cd C:\Users\ciame\scraper_olx
ssh pi@192.168.1.143 "mkdir -p /home/pi/scraper_olx"
scp main.py requirements.txt run_scraper.sh pi@192.168.1.143:/home/pi/scraper_olx/
scp -r deploy pi@192.168.1.143:/home/pi/scraper_olx/
```

Podaj hasło użytkownika Raspberry, gdy SSH/SCP o nie poprosi. Przy pierwszym połączeniu potwierdź klucz hosta tylko wtedy, gdy rozpoznajesz swoje Raspberry i jego adres.

Jeśli przenosisz działający scraper z innego komputera i chcesz zachować dotychczasową listę ogłoszeń, utwórz katalog danych na Raspberry (`mkdir -p /home/pi/scraper_olx/output`) i skopiuj pliki JSON. Nie kopiuj katalogu `output/olx_browser_profile`; zawiera profil przeglądarki i nie jest potrzebny do przeniesienia bazy.

```powershell
scp output/accepted.json output/rejected.json output/state.json pi@192.168.1.143:/home/pi/scraper_olx/output/
```

Przy zupełnie nowej instalacji pomiń kopiowanie danych; skrypt utworzy pliki podczas pierwszego przebiegu.

`.env` zawiera tokeny. Nie wysyłaj go do czatu ani nie publikuj. Skrypt może odświeżyć token OneDrive bezpośrednio w `.env` na Raspberry, więc lokalna kopia może być starsza. Najbezpieczniej utworzyć plik bezpośrednio na Raspberry, zamiast przesyłać go przez czat lub do repozytorium.

Po skopiowaniu plików zaloguj się z PowerShella:

```powershell
ssh pi@192.168.1.143
```

Poniższe polecenia wykonuj już w terminalu SSH na Raspberry:

```sh
cd /home/pi/scraper_olx
sudo apt update
sudo apt install -y python3-venv dos2unix
mkdir -p output
nano .env
```

W edytorze `nano` wpisz potrzebne ustawienia, zapisz przez `Ctrl+O`, `Enter`, a następnie wyjdź przez `Ctrl+X`. Ustaw tokeny, których używasz; wartości opcjonalne można pominąć:

```dotenv
ONEDRIVE_CLIENT_ID=tu_wstaw_client_id
ONEDRIVE_REFRESH_TOKEN=tu_wstaw_refresh_token
ONEDRIVE_UPLOAD_FOLDER=olx
TELEGRAM_BOT_TOKEN=tu_wstaw_token_bota
TELEGRAM_CHAT_ID=tu_wstaw_id_czatu
MAX_PAGES=30
MAX_EMPTY_PAGES=2
KEEP_MISSING=10
OLX_HEADLESS=1
```

OneDrive jest opcjonalny. Jeśli go nie używasz, usuń lub zakomentuj obie zmienne `ONEDRIVE_CLIENT_ID` i `ONEDRIVE_REFRESH_TOKEN`; dane będą zapisywane lokalnie na Raspberry. Telegram również jest opcjonalny; bez `TELEGRAM_BOT_TOKEN` i `TELEGRAM_CHAT_ID` powiadomienia nie będą wysyłane. Nie dodawaj cudzysłowów wokół wartości i nie zostawiaj przykładowych tekstów `tu_wstaw_...`.

Następnie dokończ instalację na Raspberry:

```sh
dos2unix run_scraper.sh
chmod 700 run_scraper.sh
chmod 600 .env
python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
./venv/bin/python -m playwright install chromium
sudo timedatectl set-timezone Europe/Warsaw
```

Jeśli Chromium zgłosi brakujące biblioteki systemowe:

```sh
sudo ./venv/bin/python -m playwright install-deps chromium
```

Wykonaj próbny przebieg:

```sh
./run_scraper.sh
```

Po poprawnym teście skopiuj jednostki systemd (jeśli nie zostały jeszcze zainstalowane):

```sh
sudo install -m 644 deploy/olx-scraper.service /etc/systemd/system/
sudo install -m 644 deploy/olx-scraper.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now olx-scraper.timer
```

## Sprawdzenie harmonogramu

Aktualny harmonogram uruchamia scraper codziennie o **06:00, 11:00, 15:00 i 20:00** według lokalnego czasu Raspberry (`Europe/Warsaw`). Sprawdź timer:

```sh
systemctl list-timers olx-scraper.timer
sudo systemctl status olx-scraper.timer --no-pager
```

`list-timers` pokazuje następny termin. Stan `active (waiting)` oznacza, że timer czeka na uruchomienie. `Persistent=true` powoduje pojedynczy start po powrocie systemu, jeśli Raspberry było wyłączone w czasie zaplanowanego przebiegu.

## Ręczne uruchomienie

Aby uruchomić scraper natychmiast przez tę samą usługę co harmonogram:

```sh
sudo systemctl start olx-scraper.service
```

Polecenie może poczekać do końca przebiegu. Logi sprawdzisz tak:

```sh
sudo journalctl -u olx-scraper.service -n 100 --no-pager
```

Podgląd logów na żywo:

```sh
sudo journalctl -u olx-scraper.service -f
```

Zakończ podgląd klawiszami `Ctrl+C`. Ręczny test bez systemd to `./run_scraper.sh` uruchomiony z katalogu projektu.

## Zmiana godzin

Edytuj w VS Code `deploy/olx-scraper.timer`. Ustaw jeden wpis `OnCalendar` na każdą porę. Przykład:

```ini
OnCalendar=*-*-* 06:00:00
OnCalendar=*-*-* 11:00:00
OnCalendar=*-*-* 15:00:00
OnCalendar=*-*-* 20:00:00
```

Zapisz plik. W PowerShellu, z katalogu projektu, skopiuj go na Raspberry:

```powershell
cd C:\Users\ciame\scraper_olx
scp deploy/olx-scraper.timer pi@192.168.1.143:/home/pi/scraper_olx/deploy/olx-scraper.timer
```

Następnie w SSH zainstaluj i przeładuj timer:

```sh
cd /home/pi/scraper_olx
sudo install -m 644 deploy/olx-scraper.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart olx-scraper.timer
systemctl list-timers olx-scraper.timer
```

Sprawdź, czy `NEXT` pokazuje najbliższy oczekiwany termin. Timer korzysta ze strefy czasowej Raspberry; w razie potrzeby ustaw ją poleceniem `sudo timedatectl set-timezone Europe/Warsaw`.

## Kopiowanie przyszłych zmian

Przed kopiowaniem sprawdź, czy scraper nie działa:

```sh
sudo systemctl status olx-scraper.service --no-pager
```

Jeśli usługa jest aktywna, poczekaj na jej zakończenie. W PowerShellu, w katalogu projektu, skopiuj zmienione pliki na Raspberry. Dla zmian w `main.py` (w tym poprawki fałszywych alertów cenowych) użyj:

```powershell
cd C:\Users\ciame\scraper_olx
scp main.py pi@192.168.1.143:/home/pi/scraper_olx/main.py
```

Poprawka cen usuwa białe znaki z kwoty przed porównaniem. Naprawia to fałszywe alerty, które mogły wystąpić, gdy OLX zapisał tysiące ze spacją nierozdzielającą, np. `1 500 zł`.

Jeśli zmienił się także launcher, skopiuj go osobno:

```powershell
scp run_scraper.sh pi@192.168.1.143:/home/pi/scraper_olx/
```

Na Raspberry popraw format i uprawnienia launchera tylko wtedy, gdy był aktualizowany:

```sh
cd /home/pi/scraper_olx
dos2unix run_scraper.sh
chmod 700 run_scraper.sh
```

Jeśli zmienił się `requirements.txt`, skopiuj go z PowerShella i zaktualizuj zależności na Raspberry:

```powershell
cd C:\Users\ciame\scraper_olx
scp requirements.txt pi@192.168.1.143:/home/pi/scraper_olx/
```

```sh
cd /home/pi/scraper_olx
./venv/bin/pip install -r requirements.txt
```

Po zmianie wersji Playwrighta doinstaluj również Chromium: `./venv/bin/python -m playwright install chromium`.

Jeśli zmieniasz pliki w `deploy`, skopiuj katalog z PowerShella:

```powershell
scp -r deploy pi@192.168.1.143:/home/pi/scraper_olx/
```

Po zmianie `olx-scraper.timer` zastosuj procedurę z sekcji „Zmiana godzin”. Po zmianie `olx-scraper.service` wykonaj na Raspberry:

```sh
sudo install -m 644 deploy/olx-scraper.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart olx-scraper.timer
```

Po aktualizacji uruchom test według sekcji „Ręczne uruchomienie”. Zmiany samego `main.py` nie wymagają przeładowania systemd ani restartu timera.

Nie kopiuj `.env` rutynowo. Jeśli musisz go zmienić, upewnij się, że kopiujesz aktualne tokeny, a potem na Raspberry wykonaj `chmod 600 /home/pi/scraper_olx/.env`. Unieważnij tokeny, jeśli zostały ujawnione.

## Zatrzymanie i logi

Wyłącz automatyczne uruchomienia:

```sh
sudo systemctl disable --now olx-scraper.timer
```

Włącz je ponownie:

```sh
sudo systemctl enable --now olx-scraper.timer
```

Przydatne kontrole:

```sh
systemctl list-timers olx-scraper.timer
sudo systemctl status olx-scraper.timer --no-pager
sudo systemctl status olx-scraper.service --no-pager
```

Logi sprawdzaj poleceniami z sekcji „Ręczne uruchomienie”. Opcja `journalctl --no-pager` wyłącza widok stronicowany; poprawna nazwa zawiera końcowe `r`.