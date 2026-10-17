# simplx-rs : PoC d'un moteur d'acteurs façon simplx en Rust

Objectif : comparer la latence aller-retour inter-cœurs (ping-pong, un seul événement en vol) entre
simplx (C++) et un équivalent Rust, en se rapprochant progressivement des fonctionnalités de simplx.

## Fonctionnalités et correspondance avec simplx

| simplx | simplx-rs |
|---|---|
| sous-classe d'`Actor::Event` | n'importe quel type `Send + 'static` (taille variable, alignement ≤ 16) |
| `registerEventHandler<E>(*this)` / `onEvent(const E&)` | `ctx.register_handler::<E, Self>()` / `Handler<E>::on_event(&mut self, ctx, src, &E)` |
| `Event::Pipe(*this, dst).push<E>(...)` | `ctx.send(dst, event)` |
| `registerUndeliveredEventHandler<E>` / `onUndeliveredEvent` | `ctx.register_undelivered_handler::<E, Self>()` / `UndeliveredHandler<E>` |
| `throw ReturnToSenderException()` | `ctx.return_to_sender()` |
| `newUnreferencedActor<A>()` | `ctx.new_actor(actor)` |
| `newReferencedActor<A>()` / `ActorReference<A>` | `ctx.new_referenced_actor(actor)` / `ActorRef<A>` (appel direct : `r.borrow_mut().methode()`) |
| `referenceLocalActor<A>(id)` (`CircularReferenceException`) | `ctx.reference_local_actor::<A>(id)` (`Err(ReferenceError::Circular)`, etc.) |
| `newReferencedSingletonActor<A>()` | `ctx.singleton(\|\| actor)` |
| `TimerActor` (service `service::Timer`) / `TimerProxy` / `onTimeout()` | `TimerService` (service `TimerServiceTag`) / `Timer` / événement `Timeout` |
| `requestDestroy()` / `onDestroyRequest()` / `acceptDestroy()` | `ctx.request_destroy()` / `on_destroy_request() -> bool` |
| `registerCallback(*this)` / `onCallback()` | `ctx.register_callback()` / `on_callback()` |
| `addServiceActor<Tag, A>(core)` / `getServiceActorId<Tag>()` | `add_service::<Tag, _, _>(core, f)` / `ctx.service::<Tag>()` |
| destructeur de `Engine` (acteurs ordinaires détruits d'abord, services ensuite) | `Engine::shutdown()` / `Drop`, même ordre |

Sémantique des références, identique à simplx : un acteur référencé n'est pas détruit tant qu'une
référence existe (sa demande de destruction est différée). Un acteur créé par
`new_referenced_actor` ou `singleton` est détruit quand sa dernière référence disparaît. Une
référence circulaire est refusée. En plus de simplx : `ActorRef` n'est pas `Send`, donc le
compilateur interdit de la faire sortir de son cœur. Chaque acteur vit dans un `Rc<RefCell<A>>`, si
bien qu'un appel réentrant (A appelle B qui rappelle A) provoque un panic explicite au lieu
d'aliaser `&mut A`.

Timers, même architecture que simplx : un `TimerService` sur un cœur garde une échéance par cœur
client et teste l'horloge par callback tant qu'il en reste une. Un `TimerHub` singleton par cœur
gère tous les timers du cœur et ne demande au service que la prochaine échéance, si bien qu'un seul
message traverse les cœurs par tick. Une différence : à l'expiration, le propriétaire reçoit un
événement `Timeout` (à distinguer avec `timer.is(&t)`, qui ignore aussi un timeout envoyé avant un
réarmement ou un `unset`) au lieu d'un appel direct à `onTimeout()`. Un appel direct depuis le hub
laisserait `onTimeout` réarmer un timer pendant que le hub s'exécute, donc aliaser le hub.

Pas encore couverts : `BufferedPipe` et pushs atomiques groupés, `registerPerformanceNeutralCallback`,
connecteurs entre processus.

## Contenu

| Fichier | Rôle |
|---|---|
| `src/bytering.rs` | Ring SPSC d'enregistrements de taille variable, alignés sur 16 octets, avec un enregistrement de padding au bouclage pour garder chaque enregistrement contigu. Index `head`/`tail` sur des lignes de cache séparées, copie locale de l'index distant, publication par lots. |
| `src/engine.rs` | Le moteur : une boucle épinglée en spin actif par cœur, un ring par paire orientée de cœurs. Le handler reçoit `&E` qui pointe directement dans le ring (aucune copie à la livraison). Les événements d'un tour de boucle sont publiés en une fois à la fin du tour. |
| `src/timer.rs` | `TimerService`, `TimerHub` (singleton par cœur) et `Timer`. |
| `src/spsc.rs` | Ring SPSC typé, utilisé par le mode `raw` du benchmark. |
| `src/affinity.rs` | Épinglage via `sched_setaffinity`, sans dépendance. |
| `tests/engine.rs` | Un test par fonctionnalité : événements hétérogènes en local et entre cœurs, les quatre cas d'événement non délivré, protocole de destruction et callbacks, ordre d'arrêt services/acteurs, débordement d'un ring plein (ordre préservé), aucun événement perdu sans être détruit (y compris ceux en vol à l'arrêt). |
| `tests/refs_timers.rs` | Appel direct par référence, destruction différée tant qu'un acteur est référencé puis automatique quand il ne l'est plus, erreurs de référence (circulaire, mauvais type, autre cœur), singleton partagé par cœur et distinct entre cœurs, timers (one-shot, répétition, `unset`, jamais en avance), absence de service, arrêt avec timers armés sur plusieurs cœurs. |
| `src/bin/pingpong_bench.rs` | Benchmark, modes `engine` (Ping et service Pong) et `raw` (deux threads et deux rings, sans moteur : le plancher matériel). |
| `../bench/pingpong_cpp/` | Le même benchmark avec simplx, modes `spin` (core-actor sans `threadYield()`) et `yield` (`DefaultCoreActor`). |
| `../bench/run_pingpong.sh` | Compile les deux benchmarks et les exécute en alternance. |

Aucune dépendance externe. `cargo test --release` lance 20 tests.

## Lancer

```sh
bench/run_pingpong.sh [reps=5] [iterations=1000000] [ping_core=1] [pong_core=2]
```

Méthode : 100 k allers-retours d'échauffement puis 1 M mesurés, chacun chronométré avec `rdtsc`
(TSC calibré sur l'horloge monotone). Message de 16 octets. Dans les deux langages, le moteur n'est
arrêté qu'après la fin des mesures.

## Résultats (2026-10-17)

Machine : VM KVM, 4 vCPU Intel Xeon (Sapphire Rapids, 2,1 GHz), Ubuntu 24.04, gcc 13.3 `-O3
-march=native`, rustc 1.97 `opt-level=3`, LTO, `target-cpu=native`. Médiane de 5 exécutions
entrelacées, cœurs 1 et 2, latences en ns :

| impl | min | p50 | p90 | p99 | moyenne | allers-retours/s |
|---|---:|---:|---:|---:|---:|---:|
| simplx spin | 783 | 1342 | 1477 | 3977 | 1464 | 549 k |
| simplx yield | 696 | 1477 | 1698 | 4638 | 1660 | 497 k |
| **simplx-rs** | 194 | **612** | 782 | 998 | 688 | 1209 k |
| rust raw (plancher) | 152 | 597 | 771 | 926 | 684 | 1249 k |

(Version avec références, singletons et timers. Avant, sans ces fonctionnalités : simplx-rs p50 593
contre 573 pour le plancher, cœurs 0 et 3 : simplx spin 1357, simplx-rs 599, raw 588.)

Le moteur Rust ajoute 10 à 20 ns au plancher, simplx environ 750 ns.

À noter pendant l'ajout des références : vider la liste des acteurs déréférencés à chaque tour de
boucle (emprunt du `RefCell`, `mem::take` du `Vec`) faisait passer le min de 190 à 370 ns et le p50
d'environ 40 ns, alors que ce sont quelques écritures locales, sans partage de ligne de cache
(vérifié par les adresses). C'est remplacé par la lecture d'un `Cell<bool>` qui n'est écrit que
lorsqu'une référence disparaît. Sur un chemin critique de quelques dizaines d'instructions, chaque
écriture par tour de boucle se voit.

## D'où venaient les 300 ns du premier PoC

La première version du moteur était environ 300 ns au-dessus du mode `raw`. Ce n'était pas le coût
de la boucle sur les rings, mais du false sharing. Les états locaux `Producer` (cœur 1 → 2) et
`Producer` (cœur 2 → 1), modifiés à chaque envoi, étaient sur la même ligne de cache. Ils avaient
été alloués côte à côte par le thread principal, et un `Vec` déplacé vers un autre thread garde son
buffer. Chaque envoi invalidait donc la ligne de l'autre cœur, dans les deux sens. En mode `raw`,
chaque thread possède sa propre extrémité, d'où l'absence du problème. Correction : `Producer` et
`Consumer` sont alignés sur 128 octets.

## Pourquoi simplx est plus lent (analyse du code, pas mesuré finement)

simplx n'utilise pas de ring entre deux cœurs : il utilise une poignée de main à une seule boîte aux
lettres (`include/trz/engine/internal/parallel.h`, `node.h`) :

1. Le cœur écrivain accumule ses événements dans des pages chaînées (`writeCache`).
2. À la fin de sa boucle, il ne peut transmettre que si le lot précédent a été acquitté
   (`isWriteLocked == false`). Il échange alors ses chaînes avec la structure partagée
   `ReadWriteLocked` (plusieurs champs sur plus d'une ligne de cache, cf. le `TODO make it all fit
   in one cache line`), recycle les pages rendues, puis fait un `mfence` et passe
   `isWriteLocked = true`.
3. Le cœur lecteur voit le flag, parcourt la chaîne d'événements (des pointeurs vers des pages
   écrites par l'autre cœur), dispatche, puis fait un `mfence` et passe `isWriteLocked = false`
   pour acquitter.

Par saut, cela fait voyager plusieurs lignes de cache (flag, structure partagée, page d'événements,
acquittement retour, réinitialisation de la structure par l'écrivain) et coûte deux `mfence`. Le ring
n'en fait voyager que deux : l'index `tail` et la ligne de l'enregistrement. Avec 60 à 100 ns par
transfert de ligne sur cette machine, cela explique l'ordre de grandeur des environ 390 ns de plus
par saut. Ce protocole a des avantages réels : nombre de lots en vol illimité sans capacité fixe,
détection d'un cœur arrêté (`writeFailed`), pas de taille maximale d'événement. Mais il est
structurellement plus coûteux en latence qu'un ring. Une confirmation fine demanderait
`perf c2c` sur une machine physique.

## Limites, à lire avant de conclure

- **L'environnement est bruité.** La VM est partagée, sans `isolcpus`, sans `nohz_full`, sans
  contrôle de fréquence, et le placement des vCPU sur les cœurs physiques est inconnu. Les queues
  (p99.9 et au-delà) reflètent surtout l'hyperviseur : ne comparez que p50 et p90. Certaines
  exécutions isolées sortent des valeurs aberrantes (p50 autour de 100 à 200 ns), probablement deux
  vCPU sur le même cœur physique. À refaire sur une machine bare metal réglée pour la basse latence.
- **Différences de comportement qui restent :** le ring a une capacité fixe (64 Kio par défaut,
  avec un débordement local en cas de ring plein) et une taille maximale d'événement (la moitié du
  ring). Les erreurs dans les handlers ne sont pas interceptées (un panic arrête le processus,
  `panic = "abort"`), alors que simplx attrape les exceptions par handler.
- **Débit :** il est calculé sur le temps total de la phase mesurée. Il inclut donc le temps passé
  dans Ping entre la réception et le renvoi.
