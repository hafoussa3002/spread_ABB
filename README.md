# Suivi des OPCVM - Al Barid Bank

Application Streamlit interne avec historique permanent SFIM/BAM, analyse et exports.

## Structure

- `app.py`
- `utils/auth.py`
- `utils/config.py`
- `utils/parsers.py`
- `utils/storage.py`
- `utils/analytics.py`
- `utils/exporters.py`
- `data/` (SQLite local persistant)

## Pages

- `OCT`
- `OMLT`
- `Diversifiés`
- `Ajout d’un nouveau fonds`
- `Courbe`
- `Analyse`
- `Export`

## Points clés implémentés

- Login interne (username/password)
- Historique cumulatif permanent (`data/app.db`), conservé après redémarrage
- Upload SFIM quotidien/hebdomadaire avec filtrage ISIN
- Date SFIM lue automatiquement depuis le titre du fichier
- Upload BAM avec date officielle = date majoritaire de `Date de valeur`
- Interpolation linéaire de courbe BAM
- J / J-1 automatiques selon les dates disponibles
- KPIs (plus performant, moins performant, dates, capital global)
- Analyse (agressif, défensif, corrélation courbe)
- Export SFIM/BAM séparé, combiné, et par date

## Installation

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Lancement

```bash
streamlit run app.py
```

## Identifiants

Par défaut:

- `admin`
- `abb2026`

Vous pouvez les surcharger via `.streamlit/secrets.toml`:

```toml
APP_USER = "votre_user"
APP_PASSWORD = "votre_mot_de_passe"
```

## GitHub

```bash
git init
git add .
git commit -m "Initial delivery: OPCVM Streamlit app"
git branch -M main
git remote add origin <URL_REPO>
git push -u origin main
```
