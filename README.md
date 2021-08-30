# PyWatch

## Introduction
This is a demo version of `ALXWatch`,
a [React Recoil](https://recoiljs.org/) style state-management framework that carries out minimal incremental computation. Its enhanced version is used in Arctic Lake's core trading service for state-monitoring, asynchronous event handling and testing.

Inspired by [Build Systems à la Carte: Theory and Practice](https://www.microsoft.com/en-us/research/uploads/prod/2020/04/build-systems-jfp.pdf)

## Quick Start
See main.py.

## Main Features
- Modularized incremental computation. Allows customizing the scheduling and rebuilding algorithms separately.
- `async`-friendly design. Easy to deal with async events with the aid of synchronization primitives.
- Takes advantage of Python's type hints, to ensure smooth coding experience with modern Python IDEs.
- Clear, easy-to-use API design, assists writing clean and concise code.
