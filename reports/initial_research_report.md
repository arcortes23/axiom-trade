{
  "crypto": {
    "bars": 1000,
    "benchmarks": [
      {
        "assumptions": {
          "execution": "bar_open_entry_with_fee_and_slippage; close_mark_to_market",
          "fee_bps": 10.0,
          "initial_cash": 10000.0,
          "slippage_bps": 5.0,
          "symbol": "BTC/USDT"
        },
        "market_type": "crypto_spot",
        "metrics": {
          "annualized_return": 0.0,
          "cagr": 0.0,
          "calmar": 0.0,
          "closed_trades": 0.0,
          "cvar": 0.0,
          "expectancy": 0.0,
          "fees": 0,
          "fills": 0.0,
          "final_equity": 10000.0,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.0,
          "profit_factor": 0.0,
          "sharpe": 0.0,
          "slippage": 0,
          "sortino": 0.0,
          "total_return": 0.0,
          "turnover": 0,
          "volatility": 0.0,
          "volatility_of_returns": 0.0,
          "win_rate": 0.0
        },
        "name": "cash"
      },
      {
        "assumptions": {
          "execution": "bar_open_entry_with_fee_and_slippage; close_mark_to_market",
          "fee_bps": 10.0,
          "initial_cash": 10000.0,
          "slippage_bps": 5.0,
          "symbol": "BTC/USDT"
        },
        "market_type": "crypto_spot",
        "metrics": {
          "annualized_return": 0.16785045520057285,
          "cagr": 0.16785045520057285,
          "calmar": 0.31686777343732475,
          "closed_trades": 0.0,
          "cvar": 0.053296915093090425,
          "expectancy": 0.0,
          "fees": 0,
          "fills": 0.0,
          "final_equity": 18498.74434673303,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.5297176591351062,
          "profit_factor": 0.0,
          "sharpe": 0.5904208764811611,
          "slippage": 0,
          "sortino": 0.8889040757638375,
          "total_return": 0.8498744346733031,
          "turnover": 0,
          "volatility": 0.3931854969662031,
          "volatility_of_returns": 0.02476835819261701,
          "win_rate": 0.0
        },
        "name": "buy_hold"
      },
      {
        "assumptions": {
          "execution": "bar_open_entry_with_fee_and_slippage; close_mark_to_market",
          "fee_bps": 10.0,
          "initial_cash": 10000.0,
          "slippage_bps": 5.0,
          "symbol": "BTC/USDT"
        },
        "market_type": "crypto_spot",
        "metrics": {
          "annualized_return": 0.021329481974574405,
          "cagr": 0.021329481974574405,
          "calmar": 0.04662055294054073,
          "closed_trades": 0.0,
          "cvar": 0.0320479430452021,
          "expectancy": 0.0,
          "fees": 0,
          "fills": 0.0,
          "final_equity": 10872.66787050927,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.4575124195068591,
          "profit_factor": 0.0,
          "sharpe": 0.20601913083798612,
          "slippage": 0,
          "sortino": 0.29828073698648755,
          "total_return": 0.08726678705092716,
          "turnover": 0,
          "volatility": 0.22095659595040312,
          "volatility_of_returns": 0.013918957224384477,
          "win_rate": 0.0
        },
        "name": "dca"
      }
    ],
    "dataset_version": "sha256:e0ec5b714c172b69ac4b8d4fa8725db88d224c74408c80d70caa368dc315c9b9",
    "errors": [],
    "experiments": [
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.08702107698033767,
            "metric": "total_return",
            "overfit_penalty": 1.1055296604172626,
            "selection_basis": "validation_only",
            "selection_fitness": -2.4142542499132755,
            "train_score": 0.9023347313385122,
            "train_to_validation_degradation": 1.1055296604172626,
            "validation_score": -0.20319492907875047
          },
          "degradation": 1.1055296604172626,
          "fitness": -2.4142542499132755,
          "holdout_degradation": 0.0,
          "holdout_score": 0.08702107698033767,
          "overfit_penalty": 1.1055296604172626,
          "selection_basis": "validation_only",
          "train_score": 0.9023347313385122,
          "validation_score": -0.20319492907875047
        },
        "holdout": {
          "annualized_return": 0.11144834194985842,
          "cagr": 0.11144834194985842,
          "calmar": 0.7135189935561863,
          "closed_trades": 0.0,
          "cvar": 0.020208281114635927,
          "expectancy": 0.0,
          "fees": 5.0024999999999995,
          "fills": 1.0,
          "final_equity": 10870.210769803376,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.15619534021708195,
          "profit_factor": 0.0,
          "sharpe": 0.7958368784522456,
          "slippage": 2.4999999999992037,
          "sortino": 1.2581161221252621,
          "total_return": 0.08702107698033767,
          "turnover": 5002.499999999999,
          "volatility": 0.16773909231757694,
          "volatility_of_returns": 0.01056656960514318,
          "win_rate": 0.0
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": false
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": false,
            "trades": 1
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.20319492907875047,
            "lower": -0.20319492907875047,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.20319492907875047
          }
        },
        "strategy_id": "btc-usdt-dip",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.31067899894501627,
          "cagr": 0.31067899894501627,
          "calmar": 1.5349377495087915,
          "closed_trades": 0.0,
          "cvar": 0.03393303317269677,
          "expectancy": 0.0,
          "fees": 5.0024999999999995,
          "fills": 1.0,
          "final_equity": 19023.347313385122,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.2024049503274249,
          "profit_factor": 0.0,
          "sharpe": 1.172509999465187,
          "slippage": 2.499999999999409,
          "sortino": 1.8399786047288818,
          "total_return": 0.9023347313385122,
          "turnover": 5002.499999999999,
          "volatility": 0.25942313517155674,
          "volatility_of_returns": 0.016342121428586494,
          "win_rate": 0.0
        },
        "validation": {
          "annualized_return": -0.2499693215660842,
          "cagr": -0.2499693215660842,
          "calmar": -0.9725690661412488,
          "closed_trades": 0.0,
          "cvar": 0.027336567112125253,
          "expectancy": 0.0,
          "fees": 5.0025,
          "fills": 1.0,
          "final_equity": 7968.050709212495,
          "initial_equity": 10000.0,
          "longest_losing_streak": 0.0,
          "max_drawdown": 0.2570196094739666,
          "profit_factor": 0.0,
          "sharpe": -1.555827784464238,
          "slippage": 2.4999999999994733,
          "sortino": -1.94337438017884,
          "total_return": -0.20319492907875047,
          "turnover": 5002.5,
          "volatility": 0.1664914412251714,
          "volatility_of_returns": 0.010487974973869772,
          "win_rate": 0.0
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.047273796294318604,
            "metric": "total_return",
            "overfit_penalty": 0.3880497945282988,
            "selection_basis": "validation_only",
            "selection_fitness": -0.8524914630003653,
            "train_score": 0.3116579205845311,
            "train_to_validation_degradation": 0.3880497945282988,
            "validation_score": -0.07639187394376767
          },
          "degradation": 0.3880497945282988,
          "fitness": -0.8524914630003653,
          "holdout_degradation": 0.0,
          "holdout_score": 0.047273796294318604,
          "overfit_penalty": 0.3880497945282988,
          "selection_basis": "validation_only",
          "train_score": 0.3116579205845311,
          "validation_score": -0.07639187394376767
        },
        "holdout": {
          "annualized_return": 0.06023689597881332,
          "cagr": 0.06023689597881332,
          "calmar": 1.050935625215304,
          "closed_trades": 38.0,
          "cvar": 0.012460264695124502,
          "expectancy": -8.416725224793467,
          "fees": 78.42484656172671,
          "fills": 49.0,
          "final_equity": 10472.737962943187,
          "initial_equity": 10000.0,
          "longest_losing_streak": 8.0,
          "max_drawdown": 0.05731739845290015,
          "profit_factor": 0.5956968285004454,
          "sharpe": 0.6583640799811483,
          "slippage": 39.21151590312334,
          "sortino": 1.0700180794042462,
          "total_return": 0.047273796294318604,
          "turnover": 78424.8465617267,
          "volatility": 0.09579877947560313,
          "volatility_of_returns": 0.0060347558665705845,
          "win_rate": 0.39473684210526316
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 38
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.07639187394376767,
            "lower": -0.07639187394376767,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.07639187394376767
          }
        },
        "strategy_id": "btc-usdt-momentum",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.12090100426991701,
          "cagr": 0.12090100426991701,
          "calmar": 0.7187375870624829,
          "closed_trades": 92.0,
          "cvar": 0.018558557384000064,
          "expectancy": 32.604323839673874,
          "fees": 229.92340882079193,
          "fills": 114.0,
          "final_equity": 13116.579205845312,
          "initial_equity": 10000.0,
          "longest_losing_streak": 10.0,
          "max_drawdown": 0.16821299796500913,
          "profit_factor": 1.9600833258724857,
          "sharpe": 0.9506500634575412,
          "slippage": 114.96212073131078,
          "sortino": 1.5318158694746258,
          "total_return": 0.3116579205845311,
          "turnover": 229923.40882079193,
          "volatility": 0.1287628391868388,
          "volatility_of_returns": 0.008111296442737568,
          "win_rate": 0.358695652173913
        },
        "validation": {
          "annualized_return": -0.09573433866064296,
          "cagr": -0.09573433866064296,
          "calmar": -1.036455102469454,
          "closed_trades": 31.0,
          "cvar": 0.012481758322227133,
          "expectancy": -24.64253998186049,
          "fees": 44.28910521488616,
          "fills": 38.0,
          "final_equity": 9236.081260562323,
          "initial_equity": 10000.0,
          "longest_losing_streak": 8.0,
          "max_drawdown": 0.09236708703787236,
          "profit_factor": 0.12419838207632454,
          "sharpe": -1.5532016212102386,
          "slippage": 22.144378236126435,
          "sortino": -1.869454560058836,
          "total_return": -0.07639187394376767,
          "turnover": 44289.10521488616,
          "volatility": 0.06347472486926886,
          "volatility_of_returns": 0.003998531822436482,
          "win_rate": 0.22580645161290322
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.029383134371627984,
            "metric": "total_return",
            "overfit_penalty": 0.22498973959379942,
            "selection_basis": "validation_only",
            "selection_fitness": -0.5263230812300566,
            "train_score": 0.1486461375513417,
            "train_to_validation_degradation": 0.22498973959379942,
            "validation_score": -0.07634360204245771
          },
          "degradation": 0.22498973959379942,
          "fitness": -0.5263230812300566,
          "holdout_degradation": 0.0,
          "holdout_score": 0.029383134371627984,
          "overfit_penalty": 0.22498973959379942,
          "selection_basis": "validation_only",
          "train_score": 0.1486461375513417,
          "validation_score": -0.07634360204245771
        },
        "holdout": {
          "annualized_return": 0.03735335185384314,
          "cagr": 0.03735335185384314,
          "calmar": 0.9036457467038679,
          "closed_trades": 46.0,
          "cvar": 0.00989762869122306,
          "expectancy": 0.16531501736075147,
          "fees": 23.77136642911803,
          "fills": 53.0,
          "final_equity": 10293.83134371628,
          "initial_equity": 10000.0,
          "longest_losing_streak": 14.0,
          "max_drawdown": 0.04133627806039366,
          "profit_factor": 1.0461882649204086,
          "sharpe": 0.5788384185630222,
          "slippage": 11.884741291343941,
          "sortino": 0.8762082805349327,
          "total_return": 0.029383134371627984,
          "turnover": 23771.36642911803,
          "volatility": 0.06726228501471895,
          "volatility_of_returns": 0.004237125684830782,
          "win_rate": 0.391304347826087
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 55
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.07634360204245771,
            "lower": -0.07634360204245771,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.07634360204245771
          }
        },
        "strategy_id": "btc-usdt-trend",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.06003554559196678,
          "cagr": 0.06003554559196678,
          "calmar": 0.3514587103449459,
          "closed_trades": 83.0,
          "cvar": 0.017925910105936638,
          "expectancy": 15.142206673034861,
          "fees": 74.67517624659989,
          "fills": 99.0,
          "final_equity": 11486.461375513416,
          "initial_equity": 10000.0,
          "longest_losing_streak": 22.0,
          "max_drawdown": 0.1708182037458788,
          "profit_factor": 1.6086057352338083,
          "sharpe": 0.5608506425873424,
          "slippage": 37.336998454704,
          "sortino": 0.8428354272321265,
          "total_return": 0.1486461375513417,
          "turnover": 74675.17624659989,
          "volatility": 0.11592366151982593,
          "volatility_of_returns": 0.0073025042726068395,
          "win_rate": 0.43373493975903615
        },
        "validation": {
          "annualized_return": -0.09567449017224516,
          "cagr": -0.09567449017224516,
          "calmar": -1.1823792833311926,
          "closed_trades": 51.0,
          "cvar": 0.010656830790091287,
          "expectancy": -14.969333733815327,
          "fees": 34.66843152500744,
          "fills": 55.0,
          "final_equity": 9236.563979575423,
          "initial_equity": 10000.0,
          "longest_losing_streak": 23.0,
          "max_drawdown": 0.08091692024804031,
          "profit_factor": 0.03192726988363461,
          "sharpe": -1.4930202156057524,
          "slippage": 17.334037904113547,
          "sortino": -1.8146102513844469,
          "total_return": -0.07634360204245771,
          "turnover": 34668.431525007436,
          "volatility": 0.05654597564000243,
          "volatility_of_returns": 0.0035620616472610196,
          "win_rate": 0.19607843137254902
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.003624018717693689,
            "metric": "total_return",
            "overfit_penalty": 0.27671244757072777,
            "selection_basis": "validation_only",
            "selection_fitness": -0.6472154885193623,
            "train_score": 0.18292185419282103,
            "train_to_validation_degradation": 0.27671244757072777,
            "validation_score": -0.09379059337790674
          },
          "degradation": 0.27671244757072777,
          "fitness": -0.6472154885193623,
          "holdout_degradation": 0.0,
          "holdout_score": 0.003624018717693689,
          "overfit_penalty": 0.27671244757072777,
          "selection_basis": "validation_only",
          "train_score": 0.18292185419282103,
          "validation_score": -0.09379059337790674
        },
        "holdout": {
          "annualized_return": 0.004591422404391476,
          "cagr": 0.004591422404391476,
          "calmar": 0.052257756711447355,
          "closed_trades": 80.0,
          "cvar": 0.011166064290964095,
          "expectancy": 0.4530023403510005,
          "fees": 41.08985197385172,
          "fills": 85.0,
          "final_equity": 10036.240187176936,
          "initial_equity": 10000.0,
          "longest_losing_streak": 13.0,
          "max_drawdown": 0.08786106969237162,
          "profit_factor": 1.0517450930975305,
          "sharpe": 0.15944567522559489,
          "slippage": 20.544950455670477,
          "sortino": 0.2306628739129046,
          "total_return": 0.003624018717693689,
          "turnover": 41089.85197385172,
          "volatility": 0.07250762979214553,
          "volatility_of_returns": 0.0045675513472560715,
          "win_rate": 0.8
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 49
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.09379059337790674,
            "lower": -0.09379059337790674,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.09379059337790674
          }
        },
        "strategy_id": "btc-usdt-mean_reversion",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.07322973740989758,
          "cagr": 0.07322973740989758,
          "calmar": 0.8527815614457169,
          "closed_trades": 178.0,
          "cvar": 0.015222673243599893,
          "expectancy": 10.444990985681326,
          "fees": 134.15718438203967,
          "fills": 194.0,
          "final_equity": 11829.21854192821,
          "initial_equity": 10000.0,
          "longest_losing_streak": 7.0,
          "max_drawdown": 0.08587162377872186,
          "profit_factor": 3.16583582840738,
          "sharpe": 0.750733787199306,
          "slippage": 67.0787237733876,
          "sortino": 1.1928723641445234,
          "total_return": 0.18292185419282103,
          "turnover": 134157.18438203968,
          "volatility": 0.10089679563743409,
          "volatility_of_returns": 0.006355900698570412,
          "win_rate": 0.8932584269662921
        },
        "validation": {
          "annualized_return": -0.11725111239648311,
          "cagr": -0.11725111239648311,
          "calmar": -0.6985815889586787,
          "closed_trades": 44.0,
          "cvar": 0.0206904800092647,
          "expectancy": -0.3323772328445491,
          "fees": 48.84872146084675,
          "fills": 49.0,
          "final_equity": 9062.094066220932,
          "initial_equity": 10000.0,
          "longest_losing_streak": 15.0,
          "max_drawdown": 0.1678416869978784,
          "profit_factor": 0.973316968050887,
          "sharpe": -0.8491343394101953,
          "slippage": 24.423336058535693,
          "sortino": -1.0878342079034566,
          "total_return": -0.09379059337790674,
          "turnover": 48848.72146084675,
          "volatility": 0.1210596215267406,
          "volatility_of_returns": 0.007626039342175169,
          "win_rate": 0.5454545454545454
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.08260877793881227,
            "metric": "total_return",
            "overfit_penalty": 0.42849575575615206,
            "selection_basis": "validation_only",
            "selection_fitness": -0.9542786530321127,
            "train_score": 0.3312086142363435,
            "train_to_validation_degradation": 0.42849575575615206,
            "validation_score": -0.09728714151980855
          },
          "degradation": 0.42849575575615206,
          "fitness": -0.9542786530321127,
          "holdout_degradation": 0.0,
          "holdout_score": 0.08260877793881227,
          "overfit_penalty": 0.42849575575615206,
          "selection_basis": "validation_only",
          "train_score": 0.3312086142363435,
          "validation_score": -0.09728714151980855
        },
        "holdout": {
          "annualized_return": 0.10573843968820595,
          "cagr": 0.10573843968820595,
          "calmar": 1.7569062978177263,
          "closed_trades": 6.0,
          "cvar": 0.013981766976498233,
          "expectancy": -1.7385950560939893,
          "fees": 13.440345664728548,
          "fills": 8.0,
          "final_equity": 10826.087779388123,
          "initial_equity": 10000.0,
          "longest_losing_streak": 1.0,
          "max_drawdown": 0.06018445025756063,
          "profit_factor": 0.9206797895756753,
          "sharpe": 1.0273871526057923,
          "slippage": 6.719315462276975,
          "sortino": 1.6565990304114742,
          "total_return": 0.08260877793881227,
          "turnover": 13440.345664728547,
          "volatility": 0.10299911430054046,
          "volatility_of_returns": 0.006488334326170155,
          "win_rate": 0.6666666666666666
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 16
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.09728714151980855,
            "lower": -0.09728714151980855,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.09728714151980855
          }
        },
        "strategy_id": "btc-usdt-breakout",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.12789971670000178,
          "cagr": 0.12789971670000178,
          "calmar": 0.6721094485518584,
          "closed_trades": 39.0,
          "cvar": 0.022625215529244524,
          "expectancy": 73.75806750476985,
          "fees": 88.85739922758229,
          "fills": 50.0,
          "final_equity": 13312.086142363434,
          "initial_equity": 10000.0,
          "longest_losing_streak": 14.0,
          "max_drawdown": 0.19029596589599698,
          "profit_factor": 2.3697603581226914,
          "sharpe": 0.858094795632003,
          "slippage": 44.42844408019431,
          "sortino": 1.3554234913672856,
          "total_return": 0.3312086142363435,
          "turnover": 88857.39922758228,
          "volatility": 0.15406310203083085,
          "volatility_of_returns": 0.009705063194874963,
          "win_rate": 0.3076923076923077
        },
        "validation": {
          "annualized_return": -0.12156205435581324,
          "cagr": -0.12156205435581324,
          "calmar": -1.2076911944188111,
          "closed_trades": 13.0,
          "cvar": 0.012434945370512119,
          "expectancy": -74.83626270754466,
          "fees": 21.960301657560542,
          "fills": 16.0,
          "final_equity": 9027.128584801914,
          "initial_equity": 10000.0,
          "longest_losing_streak": 13.0,
          "max_drawdown": 0.10065657091613864,
          "profit_factor": 0.0,
          "sharpe": -2.068549291841327,
          "slippage": 10.979915845979336,
          "sortino": -2.4459851914618014,
          "total_return": -0.09728714151980855,
          "turnover": 21960.30165756054,
          "volatility": 0.06171701565463802,
          "volatility_of_returns": 0.0038878065496012475,
          "win_rate": 0.0
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.021399292442928264,
            "metric": "total_return",
            "overfit_penalty": 0.053598915720658225,
            "selection_basis": "validation_only",
            "selection_fitness": -0.16091955149264603,
            "train_score": -0.00012280433067135554,
            "train_to_validation_degradation": 0.053598915720658225,
            "validation_score": -0.05372172005132958
          },
          "degradation": 0.053598915720658225,
          "fitness": -0.16091955149264603,
          "holdout_degradation": 0.0,
          "holdout_score": 0.021399292442928264,
          "overfit_penalty": 0.053598915720658225,
          "selection_basis": "validation_only",
          "train_score": -0.00012280433067135554,
          "validation_score": -0.05372172005132958
        },
        "holdout": {
          "annualized_return": 0.027175422716462005,
          "cagr": 0.027175422716462005,
          "calmar": 0.6197858104982052,
          "closed_trades": 62.0,
          "cvar": 0.00471515847921219,
          "expectancy": 3.377266864528826,
          "fees": 7.773817783551469,
          "fills": 63.0,
          "final_equity": 10213.992924429282,
          "initial_equity": 10000.0,
          "longest_losing_streak": 24.0,
          "max_drawdown": 0.04384647446932911,
          "profit_factor": 1.6688090804438542,
          "sharpe": 0.7032747444592552,
          "slippage": 3.8869601260057585,
          "sortino": 1.3326598075118654,
          "total_return": 0.021399292442928264,
          "turnover": 7773.81778355147,
          "volatility": 0.03921512114627153,
          "volatility_of_returns": 0.0024703204330072536,
          "win_rate": 0.6129032258064516
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 95
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.05372172005132958,
            "lower": -0.05372172005132958,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.05372172005132958
          }
        },
        "strategy_id": "btc-usdt-volatility",
        "strategy_version": 1,
        "train": {
          "annualized_return": -5.166576323945016e-05,
          "cagr": -5.166576323945016e-05,
          "calmar": -0.001337605451616583,
          "closed_trades": 237.0,
          "cvar": 0.004723888344682266,
          "expectancy": -0.5170397071807751,
          "fees": 24.473864876520565,
          "fills": 250.0,
          "final_equity": 9998.771956693286,
          "initial_equity": 10000.0,
          "longest_losing_streak": 69.0,
          "max_drawdown": 0.038625562700128596,
          "profit_factor": 0.8072971425553349,
          "sharpe": 0.013967221035494791,
          "slippage": 12.236392714315835,
          "sortino": 0.017331960367978055,
          "total_return": -0.00012280433067135554,
          "turnover": 24473.864876520565,
          "volatility": 0.0310428655021212,
          "volatility_of_returns": 0.0019555167167009265,
          "win_rate": 0.39662447257383965
        },
        "validation": {
          "annualized_return": -0.0675362913302564,
          "cagr": -0.0675362913302564,
          "calmar": -0.9730919972497818,
          "closed_trades": 94.0,
          "cvar": 0.009600219839774304,
          "expectancy": -5.715076601205307,
          "fees": 10.13416322775791,
          "fills": 95.0,
          "final_equity": 9462.782799486704,
          "initial_equity": 10000.0,
          "longest_losing_streak": 94.0,
          "max_drawdown": 0.06940380921961338,
          "profit_factor": 0.0,
          "sharpe": -1.2023253846229676,
          "slippage": 5.0669511098566185,
          "sortino": -1.4817036876958072,
          "total_return": -0.05372172005132958,
          "turnover": 10134.16322775791,
          "volatility": 0.05102252897559828,
          "volatility_of_returns": 0.003214117212643338,
          "win_rate": 0.0
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.03257058247947486,
            "metric": "total_return",
            "overfit_penalty": 0.15330981297288104,
            "selection_basis": "validation_only",
            "selection_fitness": -0.33537876553333235,
            "train_score": 0.12455067338531078,
            "train_to_validation_degradation": 0.15330981297288104,
            "validation_score": -0.02875913958757026
          },
          "degradation": 0.15330981297288104,
          "fitness": -0.33537876553333235,
          "holdout_degradation": 0.0,
          "holdout_score": 0.03257058247947486,
          "overfit_penalty": 0.15330981297288104,
          "selection_basis": "validation_only",
          "train_score": 0.12455067338531078,
          "validation_score": -0.02875913958757026
        },
        "holdout": {
          "annualized_return": 0.041422646712922395,
          "cagr": 0.041422646712922395,
          "calmar": 0.9044910050875528,
          "closed_trades": 29.0,
          "cvar": 0.008181593748728256,
          "expectancy": 11.225715076629807,
          "fees": 7.519587064395874,
          "fills": 30.0,
          "final_equity": 10325.705824794748,
          "initial_equity": 10000.0,
          "longest_losing_streak": 4.0,
          "max_drawdown": 0.04579663753418175,
          "profit_factor": 4.9857606241741275,
          "sharpe": 0.7266182322414781,
          "slippage": 3.7598776165404364,
          "sortino": 1.1842946495812083,
          "total_return": 0.03257058247947486,
          "turnover": 7519.587064395874,
          "volatility": 0.0581854741976622,
          "volatility_of_returns": 0.003665340348651897,
          "win_rate": 0.8620689655172413
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 28
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.02875913958757026,
            "lower": -0.02875913958757026,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.02875913958757026
          }
        },
        "strategy_id": "btc-usdt-rsi",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.05062308087538625,
          "cagr": 0.05062308087538625,
          "calmar": 0.6043661536424645,
          "closed_trades": 146.0,
          "cvar": 0.011522241048677091,
          "expectancy": 8.463146221500143,
          "fees": 28.331062087000497,
          "fills": 147.0,
          "final_equity": 11245.506733853108,
          "initial_equity": 10000.0,
          "longest_losing_streak": 1.0,
          "max_drawdown": 0.08376226989265556,
          "profit_factor": 429.29551332219717,
          "sharpe": 0.6928458361415895,
          "slippage": 14.16583275443344,
          "sortino": 1.0193742410806508,
          "total_return": 0.12455067338531078,
          "turnover": 28331.062087000497,
          "volatility": 0.07538663145492941,
          "volatility_of_returns": 0.0047489114049672045,
          "win_rate": 0.9931506849315068
        },
        "validation": {
          "annualized_return": -0.03627814194372048,
          "cagr": -0.03627814194372048,
          "calmar": -0.6008646886120195,
          "closed_trades": 23.0,
          "cvar": 0.008569102441780363,
          "expectancy": 0.6548068141660709,
          "fees": 6.1932932679280945,
          "fills": 28.0,
          "final_equity": 9712.408604124297,
          "initial_equity": 10000.0,
          "longest_losing_streak": 5.0,
          "max_drawdown": 0.06037655836877678,
          "profit_factor": 1.1907830751408859,
          "sharpe": -0.6020702948182515,
          "slippage": 3.0959473840895164,
          "sortino": -0.8541467884587671,
          "total_return": -0.02875913958757026,
          "turnover": 6193.293267928094,
          "volatility": 0.05852998308128102,
          "volatility_of_returns": 0.003687042368425894,
          "win_rate": 0.782608695652174
        }
      },
      {
        "evaluation": {
          "attribution": {
            "holdout_degradation_report_only": 0.0,
            "holdout_score_report_only": 0.0732245285878903,
            "metric": "total_return",
            "overfit_penalty": 0.19189662973190202,
            "selection_basis": "validation_only",
            "selection_fitness": -0.45340588654129865,
            "train_score": 0.12228400265440742,
            "train_to_validation_degradation": 0.19189662973190202,
            "validation_score": -0.0696126270774946
          },
          "degradation": 0.19189662973190202,
          "fitness": -0.45340588654129865,
          "holdout_degradation": 0.0,
          "holdout_score": 0.0732245285878903,
          "overfit_penalty": 0.19189662973190202,
          "selection_basis": "validation_only",
          "train_score": 0.12228400265440742,
          "validation_score": -0.0696126270774946
        },
        "holdout": {
          "annualized_return": 0.09361501829699992,
          "cagr": 0.09361501829699992,
          "calmar": 1.3733951268582594,
          "closed_trades": 8.0,
          "cvar": 0.01564226085813525,
          "expectancy": 91.53066073486308,
          "fees": 81.19147036007172,
          "fills": 16.0,
          "final_equity": 10732.245285878904,
          "initial_equity": 10000.0,
          "longest_losing_streak": 1.0,
          "max_drawdown": 0.06816320843598087,
          "profit_factor": 2.1650220205142676,
          "sharpe": 0.7452569095686594,
          "slippage": 40.59594868820751,
          "sortino": 1.2534500086568203,
          "total_return": 0.0732245285878903,
          "turnover": 81191.47036007172,
          "volatility": 0.1316441547508523,
          "volatility_of_returns": 0.008292802262525175,
          "win_rate": 0.5
        },
        "quality": "MEDIUM",
        "rejected": true,
        "rejection_reason": "non-positive validation selection fitness",
        "robustness": {
          "minimum_sample": {
            "checks": {
              "observations": true,
              "trades": true
            },
            "count": 200,
            "min_observations": 10,
            "min_trades": 2,
            "passed": true,
            "trades": 26
          },
          "validation_score_ci": {
            "confidence": 0.95,
            "count": 1,
            "estimate": -0.0696126270774946,
            "lower": -0.0696126270774946,
            "resamples": 256,
            "statistic": "mean",
            "upper": -0.0696126270774946
          }
        },
        "strategy_id": "btc-usdt-volume_filter",
        "strategy_version": 1,
        "train": {
          "annualized_return": 0.049731657928450135,
          "cagr": 0.049731657928450135,
          "calmar": 0.29499893608044436,
          "closed_trades": 46.0,
          "cvar": 0.02164355197570273,
          "expectancy": 26.58347883791457,
          "fees": 497.3182292125335,
          "fills": 92.0,
          "final_equity": 11222.840026544074,
          "initial_equity": 10000.0,
          "longest_losing_streak": 6.0,
          "max_drawdown": 0.1685824992768402,
          "profit_factor": 1.2210346813637976,
          "sharpe": 0.4057366253957407,
          "slippage": 248.6596068107069,
          "sortino": 0.630774026522772,
          "total_return": 0.12228400265440742,
          "turnover": 497318.22921253345,
          "volatility": 0.14564964376156633,
          "volatility_of_returns": 0.009175065141387016,
          "win_rate": 0.32608695652173914
        },
        "validation": {
          "annualized_return": -0.08732114793340817,
          "cagr": -0.08732114793340817,
          "calmar": -1.1827825112684713,
          "closed_trades": 13.0,
          "cvar": 0.015048680455693741,
          "expectancy": -53.54817467499607,
          "fees": 125.52672372370917,
          "fills": 26.0,
          "final_equity": 9303.873729225053,
          "initial_equity": 10000.0,
          "longest_losing_streak": 4.0,
          "max_drawdown": 0.07382688457217793,
          "profit_factor": 0.4916697721940169,
          "sharpe": -1.1910680898330621,
          "slippage": 62.76323490276885,
          "sortino": -1.466012330795917,
          "total_return": -0.0696126270774946,
          "turnover": 125526.72372370918,
          "volatility": 0.07436803356862498,
          "volatility_of_returns": 0.00468474576941631,
          "win_rate": 0.3076923076923077
        }
      }
    ],
    "instrument": "BTC/USDT",
    "instrument_metadata": {
      "base_asset": "BTC",
      "category": null,
      "contract_size": 1.0,
      "currency": "USDT",
      "expiry": null,
      "extra": {
        "isSpotTradingAllowed": true,
        "permissions": [],
        "quoteOrderQtyMarketAllowed": true,
        "status": "TRADING"
      },
      "lot_size": 1e-05,
      "market_id": null,
      "market_type": "crypto_spot",
      "provider": "binance",
      "question": null,
      "quote_asset": "USDT",
      "resolution_criteria": null,
      "symbol": "BTCUSDT",
      "tags": [],
      "tick_size": 0.01
    },
    "instrument_metadata_available": true,
    "limitations": [
      "Binance OHLCV does not provide historical order-book depth in this workflow.",
      "Next-bar OHLCV execution is an approximation; results are not a live-profit claim.",
      "Candidate selection and rejection use validation only; locked holdout values are report-only."
    ],
    "market_type": "crypto_spot",
    "provider": "binance",
    "simulation_quality": "MEDIUM"
  },
  "generated_at": "2026-09-04T06:19:54.585904+00:00",
  "limitations": [
    "Binance OHLCV does not provide historical order-book depth in this workflow.",
    "Next-bar OHLCV execution is an approximation; results are not a live-profit claim.",
    "Candidate selection and rejection use validation only; locked holdout values are report-only.",
    "Public Polymarket histories are price-only unless a source supplies timestamped depth; this report labels such samples PRICE_PROXY.",
    "The independent-market count is a deduplicated event/question/expiry grouping proxy, not a statistical independence claim.",
    "Market price is used only as a calibration baseline; no LLM opinion is used as a probability model.",
    "Bucket ROI is descriptive and does not establish executable profitability.",
    "Historical order-book execution requires timestamped books; current books are never backfilled into history."
  ],
  "prediction": {
    "benchmarks": [
      {
        "assumptions": {
          "bins": 10,
          "execution": "no trading; forecast-only"
        },
        "market_type": "prediction",
        "metrics": {
          "brier": 0.08684642741935483,
          "ece": 0.13451290322580642,
          "log_loss": 0.24942143590146398,
          "observations": 155.0
        },
        "name": "market_mid"
      },
      {
        "assumptions": {
          "bins": 10,
          "execution": "no trading; forecast-only"
        },
        "market_type": "prediction",
        "metrics": {
          "brier": 0.25,
          "ece": 0.03548387096774197,
          "log_loss": 0.6931471805599453,
          "observations": 155.0
        },
        "name": "constant_0.5"
      },
      {
        "assumptions": {
          "bins": 10,
          "execution": "no trading; forecast-only"
        },
        "market_type": "prediction",
        "metrics": {
          "brier": 0.0,
          "ece": 0.0,
          "log_loss": 0.0,
          "observations": 0.0
        },
        "name": "model"
      }
    ],
    "calibration": {
      "brier": 2.499999999999697e-07,
      "ece": 0.0004999999999999698,
      "log_loss": 0.0005001250416822429,
      "observations": 20
    },
    "errors": [],
    "historical_order_books_available": false,
    "independence_method": "unique event IDs or normalized question and expiry",
    "independent_resolved_markets": 20,
    "limitations": [
      "Public Polymarket histories are price-only unless a source supplies timestamped depth; this report labels such samples PRICE_PROXY.",
      "The independent-market count is a deduplicated event/question/expiry grouping proxy, not a statistical independence claim.",
      "Market price is used only as a calibration baseline; no LLM opinion is used as a probability model.",
      "Bucket ROI is descriptive and does not establish executable profitability.",
      "Historical order-book execution requires timestamped books; current books are never backfilled into history."
    ],
    "liquidity": {
      "mean_absolute_error": 0.0,
      "note": "liquidity association is descriptive; historical depth was unavailable",
      "observations": 0
    },
    "market_type": "prediction",
    "markets_requested": 20,
    "markets_with_history": 20,
    "model_version": "market-price-baseline-v1",
    "multi_horizon_calibration": {
      "1d": {
        "brier": 0.0,
        "count": 0,
        "ece": 0.0,
        "horizon_seconds": 86400,
        "log_loss": 0.0
      },
      "30d": {
        "brier": 0.0,
        "count": 0,
        "ece": 0.0,
        "horizon_seconds": 2592000,
        "log_loss": 0.0
      },
      "7d": {
        "brier": 0.0,
        "count": 0,
        "ece": 0.0,
        "horizon_seconds": 604800,
        "log_loss": 0.0
      }
    },
    "price_buckets": {
      "0-1c": {
        "count": 18,
        "lower": 0.0,
        "mean_price": 0.0007500000000000003,
        "mean_roi": -1.0,
        "resolved_count": 18,
        "upper": 0.01,
        "win_rate": 0.0,
        "wins": 0
      },
      "1-2c": {
        "count": 0,
        "lower": 0.01,
        "mean_price": 0.0,
        "mean_roi": 0.0,
        "resolved_count": 0,
        "upper": 0.02,
        "win_rate": 0.0,
        "wins": 0
      },
      "10-20c": {
        "count": 0,
        "lower": 0.1,
        "mean_price": 0.0,
        "mean_roi": 0.0,
        "resolved_count": 0,
        "upper": 0.2,
        "win_rate": 0.0,
        "wins": 0
      },
      "2-5c": {
        "count": 41,
        "lower": 0.02,
        "mean_price": 0.02295121951219511,
        "mean_roi": -1.0,
        "resolved_count": 41,
        "upper": 0.05,
        "win_rate": 0.0,
        "wins": 0
      },
      "20-50c": {
        "count": 4,
        "lower": 0.2,
        "mean_price": 0.34375,
        "mean_roi": -0.16666666666666663,
        "resolved_count": 4,
        "upper": 0.5,
        "win_rate": 0.25,
        "wins": 1
      },
      "5-10c": {
        "count": 1,
        "lower": 0.05,
        "mean_price": 0.05,
        "mean_roi": -1.0,
        "resolved_count": 1,
        "upper": 0.1,
        "win_rate": 0.0,
        "wins": 0
      },
      "50-100c": {
        "count": 91,
        "lower": 0.5,
        "mean_price": 0.7325274725274723,
        "mean_roi": 0.3385024246835296,
        "resolved_count": 91,
        "upper": 1.0,
        "win_rate": 0.9010989010989011,
        "wins": 82
      }
    },
    "provider": "polymarket",
    "repricing": {
      "markets": 7,
      "mean_reversion_fraction": 0.0,
      "note": "adjacent price changes are not independent bets"
    },
    "research_quality": "PRICE_PROXY",
    "resolved_market_groups": 20,
    "simulation_quality": "MEDIUM",
    "time_to_resolution": {
      "buckets": {
        "1d_7d": {
          "count": 0.0,
          "mean_roi": 0.0
        },
        "7d_30d": {
          "count": 0.0,
          "mean_roi": 0.0
        },
        "over_30d": {
          "count": 0.0,
          "mean_roi": 0.0
        },
        "under_1d": {
          "count": 20.0,
          "mean_roi": -0.44972486243121557
        }
      },
      "mean_roi": -0.4497248624312156,
      "mean_seconds_to_expiry": 0.0,
      "note": "last observed price and expiry are price-history approximations",
      "observations": 20
    }
  }
}
