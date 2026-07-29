---
title: 'Soft-skills pentru arhitectul software — exerciții practice per skill (Comunicare, Liderșip, Negociere, Colaborare, Mentorat, Influență, Auto-dezvoltare)'
tags: [org, book-summary, soft-skills, software-architecture, exercises, practice, leadership, communication, negotiation, romanian, architect-in-charge]
author: adrianb
sessions: []
created: '2026-07-29T21:59:30.984Z'
updated: '2026-07-29T21:59:30.984Z'
importanceScore: 0.7
---

## Executive Summary

Varianta **cu exerciții practice** a compendiului de soft-skills pentru arhitecți: aceleași skill-uri sintetizate din 6 cărți, dar fiecare skill are 2 exerciții concrete, acționabile (de aplicat la job). DE CE contează: transformă principiile teoretice în drill-uri repetabile pentru dezvoltarea deliberată a competențelor de comunicare/leadership/negociere. Sursă: `/home/adrianb/_/CartiSiEbook/IT/architecture/soft-skills-arhitect-cu-exercitii.org`. Descrierea completă a fiecărui skill + referințele bibliografice sunt în memoria-pereche `org/knowledge/soft-skills-pentru-arhitectul-software-compendiu-comunicare-lider-ip-negociere-c`.

## [Comunicare]

- **Comunicarea arhitecturii**: (1) explică un modul nou pe whiteboard în max 10 min, doar schițe, fără text predefinit; (2) redactează un tradeoff (ex. securitate vs. latență) pe o singură pagină, în termeni de business.
- **Cele 7 C-uri**: (1) rescrie un email de status lung aplicând cele 7 C-uri, la jumătate din lungime; (2) explică o problemă de design în max 3 fraze cu date concrete.
- **Ascultare activă**: (1) oglindirea — reformulează "Dacă am înțeles bine, propunerea ta e să..." și așteaptă confirmarea; (2) notițe doar pe hârtie, telefon/laptop închise.
- **Empatie**: (1) la o greșeală de developer, întrebări deschise despre blocaj în loc de critică de cod; (2) notează 3 motive pentru care propunerea unui stakeholder are sens din perspectiva lui.
- **Comunicare cu stakeholderii**: (1) "5 De Ce-uri" cu un PO (15 min) pentru business goal-ul real; (2) mini-atelier de definire a scenariilor, fără discuții despre DB/framework.
- **Metafore vizuale (pirate ship)**: (1) analogie din lumea reală la onboarding (rețea de drumuri vs. microservicii); (2) diagramă pe un ecran care arată doar valoarea finală.
- **Scriere pentru oameni ocupați**: (1) email TL;DR — decizia solicitată în primele 2 rânduri; (2) rezumă o modificare de infra în max 100 cuvinte, axat pe termene/riscuri.
- **Emphasis Over Completeness**: (1) șterge 50% din text/detalii dintr-un slide de design; (2) diagramă C4 Level 2 cu nivel de detaliu consistent.
- **Explaining Stuff**: (1) explică Event Sourcing pornind de la registrul contabil fizic, fără jargon în primele 5 min; (2) teach-back — cere colegului să rezume ce a înțeles.
- **Schițe / diagrame**: (1) schița de 5 min de mână a componentelor (zonele neschițabile = probleme latente); (2) înlocuiește un document text cu o diagramă colaborativă.
- **Prezentări**: (1) susține demo-ul stând în picioare (energie/prezență); (2) înregistrează-te 3 min, numără cuvintele de umplutură.
- **NVC**: (1) reformulează 3 comentarii de code review cu cele 4 componente NVC; (2) transformă "Voi întotdeauna ignorați testele" în observație+sentiment+nevoie+cerere.
- **Rationale**: (1) secțiune obligatorie "De ce am ales această cale (Trade-offs)" în fiecare tichet Jira major; (2) retrospectivă de 200 cuvinte pe asumpții vechi false.
- **Claritate + liderșip**: (1) briefing lunar de 15 min despre viziune; (2) prezintă o decizie ca propunere deschisă la feedback (cere să găsească defecte).

## [Liderșip]

- **Liderșip tehnic**: (1) manifest cu 5 principii tehnice + consens; (2) spike ghidat — seniorul scrie prototipul, tu dai ghidare.
- **Servant leadership**: (1) la daily, întreabă "cu ce blocaj vă pot ajuta azi?"; (2) facilitare tăcută — nu vorbi în primele 15 min de design.
- **Să te urmeze**: (1) prototip PoC stabil pentru o tehnologie controversată; (2) recunoaște imediat când propunerea ta e greșită.
- **Preluarea responsabilității**: (1) post-mortem no-blame; (2) jurnal al deciziilor asumate + ce asumpție a fost greșită.
- **Focus pe oameni**: (1) shadowing cu QA/DevOps o oră; (2) laudă specifică publică pe chat.
- **Delegare**: (1) setează doar interfața+performanța, lasă designul intern; (2) 2-3 checkpoint-uri, libertate în rest.
- **Conducerea schimbării**: (1) micro-experiment pe durată determinată (ex. 2 sprinturi de ADR markdown); (2) argument de business de 5 min (time-to-market, costuri).
- **Lider prin exemplu**: (1) respectă strict regulile proprii (primul tău commit cu teste complete); (2) punctualitate + atitudine calmă sub presiune.
- **Influență (Carnegie)**: (1) prezintă un standard nou din perspectiva devs (mai puține bug-uri de vineri); (2) validează punctul celuilalt înainte de a contrazice.

## [Negociere & decizii]

- **Negociere formală/informală**: (1) definește-ți BATNA înainte de negociere; (2) scrie interesele fiecărei echipe pe whiteboard, caută a treia opțiune.
- **"Negociezi mai des"**: (1) contra-ofertă condiționată ("livrăm la dată doar dacă eliminăm aceste 2 funcționalități"); (2) pauză de 5 sec de analiză înainte de a răspunde.
- **Echilibrarea intereselor**: (1) matrice opțiuni × stakeholderi (CFO/Devs/Securitate); (2) întâlnire de arbitraj pe limite de cost.
- **Luarea deciziilor**: (1) tabel de decizie — 3 opțiuni × 4 criterii (reversibilitate/cost/efort/mentenabilitate); (2) calculul costului opțiunii (a lăsa deschisă schimbarea DB).
- **Question Everything**: (1) identifică 3 asumpții implicite într-o specificație; (2) Pre-Mortem — "sistemul a picat în prima zi, de ce?".
- **Facilitarea deciziilor**: (1) facilitare prin consent ("are cineva o obiecție bazată pe dovezi?"); (2) propuneri anonime pe bilețele înainte de discuție.

## [Colaborare]

- **Proprietate colectivă**: (1) RFC asincron deschis o săptămână la comentarii; (2) "Mob Architecture" — 2h de scris structura împreună.
- **Rezolvarea conflictelor**: (1) depersonalizare — pro/contra pe whiteboard fără nume; (2) intervenție de 24h la o dispută pe PR.
- **Remote / culturi**: (1) ghid de comunicare asincronă; (2) întâlnire informală de 15 min de cunoaștere.
- **Respect, egalitate, umilință**: (1) recunoaște deschis ce nu știi, cere ajutorul experților; (2) invită juniorii să critice designul tău.
- **Curiozitate**: (1) Brown Bag bilunar; (2) 5 Whys — 30 min pe cum a permis arhitectura un bug.
- **Motivație intrinsecă**: (1) "Tech Debt Day" cu autonomie totală; (2) blocuri de flow de 2h fără notificări.
- **Lucrul în echipă**: (1) Conway's Law — compară diagrama de echipe cu cea de componente; (2) aliniere inter-echipe săptămânală de 15 min pe API.

## [Mentorat]

- **Mentorat**: (1) 1-la-1 bilunar axat pe cariera pe termen lung, nu pe bug-uri; (2) ghidare pe soft skills (pregătire de prezentare/negociere).
- **Teaching others**: (1) pair programming pedagogic (juniorul la tastatură); (2) tutorial intern pe wiki pentru o rută de API.
- **Share Knowledge**: (1) pagină wiki "Lessons Learned"; (2) sinteză tehnică lunară cu 3 articole/cărți.
- **Reuse is about people**: (1) demo practic de reutilizare a unei librării interne; (2) interviu de feedback cu cei care preferă cod de la zero.

## [Influență organizațională]

- **Părți non-tehnice**: (1) pitch de valoare către marketing (campanii mai rapide); (2) tradu indicatorii tehnici ("RPS 5000" → "5000 comenzi concomitente").
- **Context de business**: (1) shadowing 30 min cu un utilizator intern; (2) chestionează valoarea noului feature cu PM-ul.
- **Problema nu e tehnică**: (1) conversație empatică de remediere în loc de tool nou; (2) sesiune de aliniere Dev-QA.
- **Stakeholder map**: (1) matrice Interes vs. Influență + strategie de comunicare; (2) interviu de 10 min cu un stakeholder din Securitate/Infra.
- **Office politics**: (1) harta influenței informale (implică liderii informali din timp); (2) analiza presiunilor politice/bugetare din spatele unei decizii.
- **Comunicare transparentă**: (1) canal public de decizii arhitecturale + tradeoff-uri; (2) raportare onestă a erorilor.

## [Auto-dezvoltare]

- **Învățare continuă**: (1) o oră/vineri de studiu blocată în calendar; (2) 30 min/săptămână de lectură de cod open-source.
- **Legea instrumentului**: (1) scrie 3 argumente pentru o tehnologie ALTA decât preferata ta; (2) elimină 2 instrumente complexe dintr-un design (cache/coadă).
- **CV vs. cerințe**: (1) document de o pagină cu valoarea pentru proiect (nu pentru carieră); (2) calculează costul de mentenanță/infra pe un an.
- **Feedback**: (1) sondaj anonim pe claritatea unei arhitecturi noi; (2) întreabă un coleg "ce aș fi putut explica mai clar?".
- **Incertitudine**: (1) spike de o zi (throwaway code) când cerințele sunt incerte; (2) proiectare pentru adaptabilitate în spatele unei interfețe generice.
- **Responsabilitate + bunăstare**: (1) rutină de final de zi (10 min de bilanț + planificare); (2) sesiuni Pomodoro (25/50 min) fără întreruperi.

## Referințe
Aceleași 6 surse ca în memoria-pereche: [Handbook] Ingeno 2018 · [97Things] Monson-Haefel 2009 · [Elevator] Hohpe 2020 · [DesignIt] Keeling 2017 · [AgileArch] Rajesh R V 2021 · [SAfD1] Simon Brown 2018.