# simplx-rs : PoC d'un moteur d'acteurs façon simplx en Rust

Objectif : comparer la latence aller-retour inter-cœurs (ping-pong, un seul événement en vol) entre
simplx (C++) et un équivalent Rust minimal.

## Contenu

| Fichier | Rôle |
|---|---|
| `src/spsc.rs` | Ring SPSC lock-free borné : index `head`/`tail` sur des lignes de cache séparées, copie locale de l'index distant, écritures par lots (`write` + `commit`). `Producer`/`Consumer` non clonables : un seul écrivain et un seul lecteur garantis à la compilation. |
| `src/engine.rs` | Mini-engine : une boucle par cœur (thread épinglé, spin actif), acteurs possédés par leur cœur (pas `Send`), maillage complet de rings SPSC (une par paire orientée de cœurs). Les événements émis pendant un tour de boucle sont publiés en une fois à la fin du tour, comme dans simplx. |
| `src/affinity.rs` | Épinglage via `sched_setaffinity`, sans dépendance. |
| `src/bin/pingpong_bench.rs` | Benchmark, modes `engine` (Ping et Pong sur le mini-engine) et `raw` (deux threads et deux rings, sans engine : le plancher matériel). |
| `../bench/pingpong_cpp/` | Le même benchmark avec simplx (service Pong, `Event::Pipe`), modes `spin` (core-actor sans `threadYield()`) et `yield` (`DefaultCoreActor`). |
| `../bench/run_pingpong.sh` | Compile les deux benchmarks et les exécute en alternance. |

Aucune dépendance externe. `cargo test --release` lance les tests de la file SPSC, dont un stress
test entre threads sur 5 M messages.

## Lancer

```sh
bench/run_pingpong.sh [reps=5] [iterations=1000000] [ping_core=1] [pong_core=2]
```

Méthode : 100 k allers-retours d'échauffement puis 1 M mesurés, chacun chronométré avec `rdtsc`
(TSC calibré sur l'horloge monotone pendant l'exécution). Message de 16 octets (`seq`, `t0`).

## Résultats (2026-10-17)

Machine : VM KVM, 4 vCPU Intel Xeon (Sapphire Rapids, 2,1 GHz), Ubuntu 24.04, gcc 13.3 `-O3
-march=native`, rustc 1.97 `opt-level=3`, LTO, `target-cpu=native`. Médiane de 5 exécutions
entrelacées, cœurs 1 et 2, latences en ns :

| impl | min | p50 | p90 | p99 | p99.9 | p99.99 | moyenne | allers-retours/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| simplx spin | 875 | 1400 | 1571 | 2182 | 23985 | 84891 | 1542 | 519 k |
| simplx yield | 637 | 1590 | 1951 | 2437 | 24385 | 75155 | 1777 | 469 k |
| **rust engine** | 266 | **890** | 1101 | 1353 | 18391 | 58830 | 932 | 945 k |
| rust raw (plancher) | 147 | 597 | 724 | 864 | 13166 | 48662 | 637 | 1312 k |

Cœurs 0 et 3 (3 exécutions) : simplx spin p50 1286, rust engine p50 621, rust raw p50 617.

En résumé : le mini-engine Rust ajoute environ 300 ns au plancher de la file SPSC brute, contre
environ 800 ns pour simplx, soit un p50 environ 1,6 fois plus bas et un débit en ping-pong environ
1,8 fois plus élevé.

## Limites, à lire avant de conclure

- **L'environnement est bruité.** La VM est partagée, sans `isolcpus`, sans `nohz_full`, sans
  contrôle de fréquence, et on ne sait pas sur quels cœurs physiques tombent les vCPU. Les queues
  (p99.9 et au-delà, max en millisecondes) reflètent surtout l'hyperviseur et l'OS : ne comparez que
  p50, p90 et p99. Certaines exécutions isolées sortent des valeurs aberrantes (un p50 de 98 ns en
  mode engine, probablement deux vCPU sur le même cœur physique). À refaire sur une machine bare
  metal réglée pour la basse latence (isolcpus, cœurs du même socket, C-states et turbo figés).
- **Les deux moteurs n'ont pas les mêmes fonctionnalités.** Le PoC Rust gère un seul type
  d'événement (un type `M` monomorphisé, copié directement dans le ring). Il n'a ni événements
  hétérogènes alloués dans des pages, ni registre de handlers par type, ni gestion des événements
  non délivrés, ni protocole de destruction des acteurs, ni services nommés, ni timers.
  simplx paie tout cela à chaque événement. Une partie de l'écart vient donc des fonctionnalités
  et pas du langage. Prochaine étape pour une comparaison juste : ajouter des événements
  hétérogènes à taille variable (ring d'octets et dispatch par type) et remesurer.
- **Débit :** il est calculé sur le temps total de la phase mesurée. Il inclut donc le temps passé
  dans Ping entre la réception et le renvoi, en plus de la latence.
