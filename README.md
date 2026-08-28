# MTGbyNico-prices

Historique journalier des prix EUR (Scryfall) utilisé par [MTG by Nico](https://github.com/MagicCode666/MTGbyNico-dist).

Ce repo n'est pas un outil autonome : c'est juste le stockage de données qui alimente le suivi de valeur de collection de l'application. Rien à installer ni exécuter ici.

## Fonctionnement

Chaque nuit, une GitHub Action (`.github/workflows/nightly.yml`) exécute `scripts/fetch_prices.py` :

1. Télécharge le bulk `default_cards` depuis l'API Scryfall
2. Extrait le prix EUR (normal / foil / etched) de chaque carte
3. Écrit un instantané du jour dans `prices/YYYY-MM-DD.json`, accompagné d'un hash `.sha256` pour vérifier l'intégrité
4. Supprime les fichiers plus anciens que la rétention définie dans `config.json` (180 jours par défaut)

L'application MTG by Nico télécharge ensuite ces fichiers au démarrage pour reconstituer l'historique de prix de ta collection, en ne conservant que les variations de prix jour après jour.

## Données

Uniquement des prix de marché publics agrégés par carte (aucune donnée personnelle). Format d'un fichier `prices/YYYY-MM-DD.json` :

```json
{
  "cards": {
    "<scryfall_id>": [eur, eur_foil, eur_etched]
  }
}
```
