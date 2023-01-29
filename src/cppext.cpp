#include <cstddef>
#include <cstdlib>
#include <cmath>
#include <charconv>
#include <string>
#include <string_view>
#include <limits>
#include <chrono>
#include <variant>
#include <vector>
#include <array>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <sstream>

#include "fmt/core.h"
#include "fmt/format.h"
#include "date/date.h"
#include "simdjson.h"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
namespace s  = std;
namespace j  = simdjson;
namespace d  = date;
namespace c  = s::chrono;
namespace f  = fmt;

using uint64 = uint64_t;
constexpr uint64 HIST_SIZE = 1920;
constexpr double FNAN = s::numeric_limits<double>::quiet_NaN();
constexpr double FMAX = s::numeric_limits<double>::max();
constexpr double FMIN = s::numeric_limits<double>::min();
constexpr double FINF = s::numeric_limits<double>::infinity();
constexpr uint64 IMAX = s::numeric_limits<uint64>::max();
constexpr uint64 IMIN = s::numeric_limits<uint64>::min();
constexpr char STIME_COLNAME[] = "sequence_time";
constexpr char DATETIME_FORMAT[] = "%Y-%m-%dT%H:%M:%S %Z";
constexpr char OUTPUT_DATETIME_FORMAT[] = "%Y-%m-%dT%H:%M:%S.000000 %Z"; //TODO: hack, because we only print epoch in seconds, this is okay

j::ondemand::parser json_parser;
//TODO: move format buffer here

enum class TradeSide : int {
  SELL=0,
  BUY=1,
  INVALID=2,
};

struct Trade {
  uint64 trade_id;
  double trade_time;
  double price;
  double size;
  TradeSide side;

  Trade() = default;
  Trade(uint64 ti, double tt, double p, double s, TradeSide ts):
    trade_id(ti), trade_time(tt), price(p), size(s), side(ts)
  {}
};

using TVectorHist = s::array<s::vector<Trade>, HIST_SIZE>;
using FVectorHist = s::array<s::vector<double>, HIST_SIZE>;
using IVectorHist = s::array<s::vector<uint64>, HIST_SIZE>;
using FValueHist  = s::array<double, HIST_SIZE>;
using IValueHist  = s::array<uint64, HIST_SIZE>;
using SimpleJsonType = s::variant<s::string,double,int64_t,uint64_t,bool,s::nullptr_t>;

uint64 next_idx(uint64 idx){
  idx = idx + 1 < HIST_SIZE ? idx + 1 : 0;
  return idx;
}

uint64 prev_idx(uint64 idx){
  idx = idx - 1 < HIST_SIZE ? idx - 1 : HIST_SIZE - 1;
  return idx;
}

s::string field_name(const s::string& pid, s::string_view subscript){
  auto buffer = f::memory_buffer();
  f::format_to(s::back_inserter(buffer), "{pid}:{sub}", f::arg("pid",pid), f::arg("sub",subscript));
  return f::to_string(buffer);
}

s::string field_name(const s::string& pid, s::string_view subscript, uint64 k, s::string_view stat){
  auto buffer = f::memory_buffer();
  f::format_to(s::back_inserter(buffer), "{pid}:{sub}_{k}{stat}", f::arg("pid",pid), f::arg("sub",subscript), f::arg("k",k), f::arg("stat",stat));
  return f::to_string(buffer);
}

double trade_timestamp_to_epoch(s::string_view datetime_str){
  s::stringstream ss; ss << datetime_str;
  d::sys_time<c::milliseconds> ms;
  ss >> d::parse(DATETIME_FORMAT, ms);
  if (not ss.fail() and not ss.bad()) return ms.time_since_epoch().count() / 1000.;
  else                                return 0.;
}

s::string epoch_to_timestamp(uint64 epoch, const s::string& format){
  d::sys_seconds tp = c::time_point_cast<c::seconds>(c::system_clock::from_time_t(epoch));
  s::string out = d::format(format.c_str(), tp);
  return out;
}

j::error_code parse_double(double& out, j::ondemand::value value){
  j::ondemand::json_type ty;
  j::error_code error = value.type().get(ty);
  if (error){
    out = FNAN;
    return error;
  }
  switch (ty){
  break; case j::ondemand::json_type::string: {
    s::string_view sv;
    error = value.get(sv);
    if (error) return error;
    s::string s = {s::begin(sv), s::end(sv)};
    s::transform(s::begin(s), s::end(s), s::begin(s), [](char c){ return s::tolower(c); });
    if (s == "inf" or s == "infinity")
      out = FINF;
    else if (s == "-inf" or s == "-infinity")
      out = FINF * -1;
    else if (sv == "nan")
      out = FNAN;
    else
      s::from_chars(s::begin(sv), s::end(sv), out);
  }
  break; case j::ondemand::json_type::number: {
    j::ondemand::number number = value.get_number();
    out = number.get_double();
  }
  break; default:
    out = FNAN;
  }
  return error;
}

j::error_code parse_uint(uint64& out, j::ondemand::value value){
  j::ondemand::json_type ty;
  j::error_code error = value.type().get(ty);
  if (error){
    out = 0;
    return error;
  }
  switch (ty){
  break; case j::ondemand::json_type::string: {
    s::string_view sv;
    error = value.get(sv);
    if (error) return error;
    s::string s = {s::begin(sv), s::end(sv)};
    s::transform(s::begin(s), s::end(s), s::begin(s), [](char c){ return s::tolower(c); });
    if (s == "inf" or s == "infinity")
      out = IMAX;
    else if (s == "-inf" or s == "-infinity")
      out = IMIN;
    else if (s == "max")
      out = IMAX;
    else if (s == "min")
      out = IMIN;
    else
      s::from_chars(s::begin(sv), s::end(sv), out);
  }
  break; case j::ondemand::json_type::number: {
    j::ondemand::number number = value.get_number();
    out = number.get_uint64();
  }
  break; default:
    out = 0;
  }
  return error;
}

j::error_code parse_string(s::string_view& out, j::ondemand::value value){
  j::error_code error = value.get(out);
  return error;
}

double compute_book_first_tick(const s::vector<double>& prices){
  if (prices.size() < 2) return 0.;
  else                   return abs(prices[0] - prices[1]);
}

double compute_book_avg_tick(const s::vector<double>& prices){
  if (prices.size() < 2) return 0.;

  uint64 count = 0;
  double s = 0.;
  for (uint64 i = 1; i < prices.size(); ++i, count++){
    if (isnan(prices[i]) or isnan(prices[i-1]))
      continue;
    s += abs(prices[i] - prices[i-1]);
  }
  return s / double(count);
}

double compute_bid_size_change(const s::vector<double>& cp, const s::vector<double>& cs, const s::vector<double>& pp, const s::vector<double>& ps){
  if (cp.size() == 0 or cs.size() == 0 or pp.size() == 0 or ps.size() == 0) return 0.;
  if (cp.size() != cs.size() or pp.size() != ps.size()) return 0.;

  double sc = 0.;
  uint64 pidx = 0;
  for (uint64 cidx = 0; cidx < cp.size(); ++cidx){
    if (cp[cidx] == pp[pidx]){
      sc += cs[cidx] - ps[pidx];
      break;
    } else if (cp[cidx] > pp[pidx]){
      sc += cs[cidx];
    } else {
      while (pidx < pp.size() and cp[cidx] < pp[pidx]){
        sc -= ps[pidx];
        pidx++;
      }
      if (pidx >= pp.size()) break;
    }
  }
  return sc;
}

double compute_ask_size_change(const s::vector<double>& cp, const s::vector<double>& cs, const s::vector<double>& pp, const s::vector<double>& ps){
  if (cp.size() == 0 or cs.size() == 0 or pp.size() == 0 or ps.size() == 0) return 0.;
  if (cp.size() != cs.size() or pp.size() != ps.size()) return 0.;

  double sc = 0.;
  uint64 pidx = 0;
  for (uint64 cidx = 0; cidx < cp.size(); ++cidx){
    if (cp[cidx] == pp[pidx]){
      sc += cs[cidx] - ps[pidx];
      break;
    } else if (cp[cidx] < pp[pidx]){
      sc += cs[cidx];
    } else {
      while (pidx < pp.size() and cp[cidx] > pp[pidx]){
        sc -= ps[pidx];
        pidx++;
      }
      if (pidx >= pp.size()) break;
    }
  }
  return sc;
}

double compute_bid_volume_change(const s::vector<double>& cp, const s::vector<double>& cs, const s::vector<double>& pp, const s::vector<double>& ps){
  if (cp.size() == 0 or cs.size() == 0 or pp.size() == 0 or ps.size() == 0) return 0.;
  if (cp.size() != cs.size() or pp.size() != ps.size()) return 0.;

  double vc  = 0.;
  uint64 pidx = 0;
  for (uint64 cidx = 0; cidx < cp.size(); ++cidx){
    if (cp[cidx] == pp[pidx]){
      vc += (cs[cidx] - ps[pidx]) * cp[cidx];
      break;
    } else if (cp[cidx] > pp[pidx]){
      vc += cs[cidx] * cp[cidx];
    } else {
      while (pidx < pp.size() and cp[cidx] < pp[pidx]){
        vc -= ps[pidx] * pp[pidx];
        pidx++;
      }
      if (pidx >= pp.size()) break;
    }
  }
  return vc;
}

double compute_ask_volume_change(const s::vector<double>& cp, const s::vector<double>& cs, const s::vector<double>& pp, const s::vector<double>& ps){
  if (cp.size() == 0 or cs.size() == 0 or pp.size() == 0 or ps.size() == 0) return 0.;
  if (cp.size() != cs.size() or pp.size() != ps.size()) return 0.;

  double vc = 0.;
  uint64 pidx = 0;
  for (uint64 cidx = 0; cidx < cp.size(); ++cidx){
    if (cp[cidx] == pp[pidx]){
      vc += (cs[cidx] - ps[pidx]) * cp[cidx];
      break;
    } else if (cp[cidx] < pp[pidx]){
      vc += cs[cidx] * cp[cidx];
    } else {
      while (pidx < pp.size() and cp[cidx] > pp[pidx]){
        vc -= ps[pidx] * pp[pidx];
        pidx++;
      }
      if (pidx >= pp.size()) break;
    }
  }
  return vc;
}

double mean(const s::vector<double>& v){
  double s = 0.;
  for (double f : v)
    if (not isnan(f))
      s += f;
  return s / v.size();
}

s::pair<double,double> mean(const s::vector<s::pair<double,double>>& v){
  double s1 = 0., s2 = 0.;
  for (const s::pair<double,double>& f : v){
    if (not isnan(f.first))
      s1 += f.first;
    if (not isnan(f.second))
      s2 += f.second;
  }
  return s::make_pair(s1 / v.size(), s2 / v.size());
}

s::array<double, 3> linear_regression(const s::vector<double>& xs, const s::vector<double>& ys){
  s::array<double, 3> ret;
  double mx = mean(xs);
  double my = mean(ys);

  double sxx = 0., sxy = 0.;
  for (uint64 i = 0; i < xs.size(); ++i){
    if (not isnan(xs[i]) and not isnan(ys[i])){
      sxx += xs[i] * xs[i];
      sxy += xs[i] * ys[i];
    }
  }
  sxx -= mx * mx * xs.size();
  sxy -= mx * my * xs.size();
  ret[1] = sxx / sxy;
  ret[0] = my - ret[1] * mx;
  ret[2] = 0.;
  for (uint64 i = 0; i < xs.size(); ++i){
    ret[2] += pow(ys[i] - (xs[i] * ret[1] + ret[0]), 2.);
  }
  ret[2] = sqrt(ret[2]);
  return ret;
}

s::array<double, 3> linear_regression(const s::vector<s::pair<double,double>>& xys){
  s::array<double, 3> ret;
  s::pair<double,double> m = mean(xys);

  double sxx = 0., sxy = 0.;
  for (uint64 i = 0; i < xys.size(); ++i){
    if (not isnan(xys[i].first) and not isnan(xys[i].second)){
      sxx += xys[i].first * xys[i].first;
      sxy += xys[i].first * xys[i].second;
    }
  }
  sxx -= m.first * m.first * xys.size();
  sxy -= m.first * m.second * xys.size();
  ret[1] = sxx / sxy;
  ret[0] = m.second - ret[1] * m.first;
  ret[2] = 0.;
  for (uint64 i = 0; i < xys.size(); ++i)
    ret[2] += pow(xys[i].second - (xys[i].first * ret[1] + ret[0]), 2.);
  ret[2] = sqrt(ret[2]);
  return ret;
}

double compute_book_imbalance(double cbprice, double cbsize, double caprice, double casize, double pbprice, double pbsize, double paprice, double pasize){
  double ge_bid_price = cbprice >= pbprice;
  double le_bid_price = cbprice <= pbprice;
  double ge_ask_price = caprice >= paprice;
  double le_ask_price = caprice <= paprice;
  double imba = ge_bid_price * cbsize - le_bid_price * pbsize + ge_ask_price * pasize - le_ask_price * casize;
  return imba;
}

//extending the concept of book imbalance to the entire order book to be more accurate
double compute_extended_book_imbalance(
    s::vector<double>& cbprices, s::vector<double>& cbsizes, s::vector<double>& caprices, s::vector<double>& casizes,
    s::vector<double>& pbprices, s::vector<double>& pbsizes, s::vector<double>& paprices, s::vector<double>& pasizes
){
  if (cbprices.size() == 0 or cbsizes.size() == 0 or caprices.size() == 0 or casizes.size() == 0 or
      pbprices.size() == 0 or pbsizes.size() == 0 or paprices.size() == 0 or pasizes.size() == 0)
    return 0.;
  if (cbprices.size() != cbsizes.size() or caprices.size() != casizes.size() or
      pbprices.size() != pbsizes.size() or paprices.size() != pasizes.size())
    return 0.;

  double ibsize = 0.;
  uint64 cidx = 0;
  for (; cidx < cbprices.size() and cbprices[cidx] > pbprices[0]; ++cidx)
    ibsize += cbsizes[cidx];

  double dbsize = 0.;
  uint64 pidx = 0;
  for (; pidx < pbprices.size() and pbprices[pidx] > cbprices[0]; ++pidx)
    dbsize += pbsizes[pidx];

  double bimbalance = ibsize - dbsize;
  if (cidx < cbprices.size() and pidx < pbprices.size() and cbprices[cidx] == pbprices[pidx])
    bimbalance += (cbsizes[cidx] - pbsizes[pidx]);

  double dasize = 0.;
  cidx = 0;
  for (; cidx < caprices.size() and caprices[cidx] < paprices[0]; ++cidx)
    dasize += casizes[cidx];

  double iasize = 0.;
  pidx = 0;
  for (; pidx < paprices.size() and paprices[pidx] < caprices[0]; ++pidx)
    iasize += pasizes[pidx];

  double aimbalance = iasize - dasize;
  if (cidx < caprices.size() and pidx < paprices.size() and caprices[cidx] == paprices[pidx])
    aimbalance += (pasizes[pidx] - casizes[cidx]);

  return bimbalance + aimbalance;
}

double compute_weighted_average_price(double bprice, double bsize, double aprice, double asize){
  if (bsize + asize == 0.) return FNAN;
  else                     return (bprice * asize + aprice * bsize) / (bsize + asize);
}

double compute_return(double price1, double price2){
  if (isnan(price1) or price1 == 0.) return FNAN;
  else                               return log(price2 / price1);
}

double compute_bidask_spread(double bid_price, double ask_price){
  if (isnan(ask_price) or ask_price == 0.) return FNAN;
  else                                     return (ask_price - bid_price) / ask_price;
}

struct PidHistDataS1 {
  // book data
  FValueHist mBookBidPrice;
  FValueHist mBookAskPrice;
  FValueHist mBookBidSize;
  FValueHist mBookAskSize;
  IValueHist mBookBidHand;
  IValueHist mBookAskHand;
  FValueHist mBookBidAskImbalance;
  FValueHist mWapPrice;
  FValueHist mBookReturn;
  FValueHist mBidAskReturn;
  FValueHist mBookBidAskSpread;

  // trade data
  TVectorHist mTrades;
  IValueHist  mTradeNBuys;
  IValueHist  mTradeNSells;
  FValueHist  mTradeBuySellDiff;
  FValueHist  mTradeSize;
  FValueHist  mTradeVolume;
  FValueHist  mTradeAvgPrice;
  FValueHist  mTradeReturn;
};

struct PidHistDataS2 {
  // book data
  FVectorHist mBookBidPrices;
  FVectorHist mBookAskPrices;
  FVectorHist mBookBidSizes;
  FVectorHist mBookAskSizes;
  IVectorHist mBookBidHands;
  IVectorHist mBookAskHands;
  IValueHist  mBookBidStackSize;
  IValueHist  mBookAskStackSize;
  FValueHist  mBookBidTick1;
  FValueHist  mBookAskTick1;
  FValueHist  mBookBidAvgTick;
  FValueHist  mBookAskAvgTick;
  FValueHist  mBookBidSizeChange;
  FValueHist  mBookAskSizeChange;
  FValueHist  mBookBidVolumeChange;
  FValueHist  mBookAskVolumeChange;
  FValueHist  mBookBidLevelSlope;
  FValueHist  mBookAskLevelSlope;
  FValueHist  mBookBidLevelIntercept;
  FValueHist  mBookAskLevelIntercept;
  FValueHist  mBookBidLevelRMSE;
  FValueHist  mBookAskLevelRMSE;
  FValueHist  mBookBidAskImbalance;
  FValueHist  mBookBidAskImbalanceExtended;
  FValueHist  mWapPrice;
  FValueHist  mBookReturn;
  FValueHist  mBookBidAskSpread;

  // trade data
  TVectorHist mTrades;
  IValueHist  mTradeNBuys;
  IValueHist  mTradeNSells;
  FValueHist  mTradeBuySellDiff;
  FValueHist  mTradeSize;
  FValueHist  mTradeVolume;
  FValueHist  mTradeAvgPrice;
  FValueHist  mTradeReturn;
};

struct ETLState {
  ETLState(): mNxtIdx(0) {
    //TODO: reduce length to 960
    mBookLengths =  {3, 9, 27, 81, 162, 324, 648, 960, 1440, 1920};
    mTradeLengths = {3, 9, 27, 81, 162, 324, 648, 960, 1440, 1920};
    //TODO: need to review this these lengths are appropriate
    mReturnLengths = {27, 81, 162, 324, 648, 960, 1440, 1920};
  }

  uint64 hist_size() const {
    return HIST_SIZE;
  }

protected:
  template <typename T>
  s::vector<s::array<double, 4>> rolling_avg_sum_max_min_multi_k(const s::array<T, HIST_SIZE>& data, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double, 4>> ret; ret.reserve(lengths.size());
    double sum = 0.;
    double s_min = FMAX;
    double s_max = FMIN;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        double rmin = s_min == FMAX ? FNAN : s_min;
        double rmax = s_max == FMIN ? FNAN : s_max;
        if (count_nan){
          s::array<double, 4> a = {sum/count,             sum, rmax, rmin};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double, 4> a = {FNAN,                  sum, rmax, rmin};
          ret.push_back(a);
        } else {
          s::array<double, 4> a = {sum/(count-nan_count), sum, rmin, rmin};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx])){
        sum += data[cur_idx];
        s_max = s::max(s_max, (double)data[cur_idx]);
        s_min = s::min(s_min, (double)data[cur_idx]);
      } else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 4>> rolling_avg_sum_max_min_aoa_multi_k(const s::array<s::vector<T>, HIST_SIZE>& data, uint64 idx, uint64 ioidx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double, 4>> ret; ret.reserve(lengths.size());
    double sum = 0.;
    double s_min = FMAX;
    double s_max = FMIN;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        double rmin = s_min == FMAX ? FNAN : s_min;
        double rmax = s_max == FMIN ? FNAN : s_max;
        if (count_nan){
          s::array<double, 4> a = {sum/count,             sum, rmax, rmin};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double, 4> a = {FNAN,                  sum, rmax, rmin};
          ret.push_back(a);
        } else {
          s::array<double, 4> a = {sum/(count-nan_count), sum, rmax, rmin};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (data[cur_idx].size() > ioidx and not isnan(data[cur_idx][ioidx])){
        sum += data[cur_idx][ioidx];
        s_max = s::max(s_max, (double)data[cur_idx][ioidx]);
        s_min = s::min(s_min, (double)data[cur_idx][ioidx]);
      } else 
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 4>> rolling_abs_avg_sum_max_min_multi_k(const s::array<T, HIST_SIZE>& data, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double, 4>> ret; ret.reserve(lengths.size());
    double sum = 0.;
    double s_min = FMAX;
    double s_max = FMIN;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        double rmin = s_min == FMAX ? FNAN : s_min;
        double rmax = s_max == FMIN ? FNAN : s_max;
        if (count_nan){
          s::array<double, 4> a = {sum/count,             sum, rmax, rmin};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double, 4> a = {FNAN,                  sum, rmax, rmin};
          ret.push_back(a);
        } else {
          s::array<double, 4> a = {sum/(count-nan_count), sum, rmax, rmin};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx])){
        sum += abs(data[cur_idx]);
        s_max = s::max(s_max, abs((double)data[cur_idx]));
        s_min = s::min(s_min, abs((double)data[cur_idx]));
      } else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 2>> rolling_ema_ems_multi_k(const s::array<T, HIST_SIZE>& data, const s::vector<double>& cache, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<s::array<double, 2>> ret;
    double val = not isnan(data[idx]) ? data[idx] : 0.;
    for (uint64 i = 0; i < lengths.size(); ++i){
      uint64 k = lengths[i];
      if (not isnan(cache[i])){
        double ema = (1. / k) * val + (1. - (1. - k)) * cache[i];
        ret.push_back(s::array<double, 2>(ema, ema * k));
      } else
        ret.push_back(s::array<double, 2>(val, val * k));
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 2>> rolling_ema_ems_aoa_multi_k(const s::array<s::vector<T>, HIST_SIZE>& data, const s::vector<double>& cache, uint64 idx, uint64 ioidx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<s::array<double, 2>> ret;
    double val = data[idx].size() > ioidx and not isnan(data[idx][ioidx]) ? data[idx][ioidx] : 0.;
    for (uint64 i = 0; i < lengths.size(); ++i){
      uint64 k = lengths[i];
      if (not isnan(cache[i])){
        double ema = (1. / k) * val + (1. - (1. - k)) * cache[i];
        ret.push_back(s::array<double, 2>(ema, ema * k));
      } else
        ret.push_back(s::array<double, 2>(val, val * k));
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 2>> rolling_abs_ema_ems_multi_k(const s::array<double, HIST_SIZE>& data, const s::vector<double>& cache, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<s::array<double, 2>> ret;
    double val = not isnan(data[idx]) ? abs(data[idx]) : 0.;
    for (uint64 i = 0; i < lengths.size(); ++i){
      uint64 k = lengths[i];
      if (not isnan(cache[i])){
        double ema = (1. / k) * val + (1. - (1. - k)) * abs(cache[i]);
        ret.push_back(s::array<double, 2>(ema, ema * k));
      } else
        ret.push_back(s::array<double, 2>(val, val * k));
    }
    return ret;
  }

  template <typename T>
  s::array<double, 2> rolling_avg_sum(const s::array<T, HIST_SIZE>& data, uint64 idx, uint64 timestamp, uint64 length){
    s::array<double, 2> ret;
    double sum = 0.;
    uint64 count = 0;
    uint64 min_timestamp = timestamp - length;
    for (uint64 i = 0, cur_idx = idx; i < length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp)
        continue;
      if (not isnan(data[cur_idx]))
        sum += data[cur_idx];
    }
    ret = {sum / count, sum};
    return ret;
  }

  template <typename T>
  s::vector<s::array<double,2>> rolling_var_std_multi_k(const s::array<T, HIST_SIZE>& data, const s::vector<s::array<double, 4>>& means, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double,2>> ret; ret.reserve(lengths.size());
    double sum = 0.;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        if (count_nan){
          double var = sum / count;
          s::array<double, 2> a = {var,  sqrt(var)};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double, 2> a = {FNAN, FNAN};
          ret.push_back(a);
        } else {
          double var = sum / (count - nan_count);
          s::array<double, 2> a = {var, sqrt(var)};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx]))
        sum += pow(data[cur_idx] - means[length_idx][0], 2.);
      else if (count_nan)
        sum += pow(0. - means[length_idx][0], 2.);
      else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double,2>> rolling_var_std_aoa_multi_k(const s::array<s::vector<T>, HIST_SIZE>& data, const s::vector<s::array<double, 4>>& means, uint64 idx, uint64 ioidx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double,2>> ret; ret.reserve(lengths.size());
    double sum = 0.;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        if (count_nan){
          double var = sum / count;
          s::array<double, 2> a = {var,  sqrt(var)};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double, 2> a = {FNAN, FNAN};
          ret.push_back(a);
        } else {
          double var = sum / (count - nan_count);
          s::array<double, 2> a = {var,  sqrt(var)};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (data[cur_idx].size() > ioidx and not isnan(data[cur_idx][ioidx]))
        sum += pow(data[cur_idx][ioidx] - means[length_idx][0], 2.);
      else if (count_nan)
        sum += pow(0. - means[length_idx][0], 2.);
      else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double,2>> rolling_skew_kurt_multi_k(const s::array<T, HIST_SIZE>& data, const s::vector<s::array<double,4>>& means, const s::vector<s::array<double,2>>& vars, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double,2>> ret; ret.reserve(lengths.size());
    double skew = 0., kurt = 0.;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        if (count_nan){
          s::array<double,2> a = {skew / count,           kurt / count};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double,2> a = {FNAN,                   FNAN};
          ret.push_back(a);
        } else {
          s::array<double,2> a = {skew/(count-nan_count), kurt/(count-nan_count)};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx])){
        skew += pow((data[cur_idx] - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((data[cur_idx] - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else if (count_nan){
        skew += pow((0. - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((0. - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double,2>> rolling_skew_kurt_aoa_multi_k(const s::array<s::vector<T>, HIST_SIZE>& data, const s::vector<s::array<double,4>>& means, const s::vector<s::array<double,2>>& vars, uint64 idx, uint64 ioidx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double,2>> ret; ret.reserve(lengths.size());
    double skew = 0., kurt = 0.;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        if (count_nan){
          s::array<double,2> a = {skew / count,           kurt / count};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double,2> a = {FNAN,                   FNAN};
          ret.push_back(a);
        } else {
          s::array<double,2> a = {skew/(count-nan_count), kurt/(count-nan_count)};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (data[cur_idx].size() > ioidx and not isnan(data[cur_idx][ioidx])){
        skew += pow((data[cur_idx][ioidx] - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((data[cur_idx][ioidx] - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else if (count_nan) {
        skew += pow((0. - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((0. - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else
        nan_count++;
    }
    return ret;
  }

  template <typename T>
  s::vector<s::array<double, 2>> rolling_abs_skew_kurt_multi_k(const s::array<T, HIST_SIZE>& data, const s::vector<s::array<double,4>>& means, const s::vector<s::array<double,2>>& vars, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths, bool count_nan=false){
    s::vector<s::array<double,2>> ret; ret.reserve(lengths.size());
    double skew = 0., kurt = 0.;
    uint64 nan_count = 0, count = 0, length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx), count++){
      if (i == lengths[length_idx]){
        if (count_nan){
          s::array<double,2> a = {skew / count,           kurt / count};
          ret.push_back(a);
        } else if (count - nan_count <= 0){
          s::array<double,2> a = {FNAN,                   FNAN};
          ret.push_back(a);
        } else {
          s::array<double,2> a = {skew/(count-nan_count), kurt/(count-nan_count)};
          ret.push_back(a);
        }
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx])){
        skew += pow((abs(data[cur_idx]) - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((data[cur_idx] - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else if (count_nan){
        skew += pow((0. - means[length_idx][0]) / vars[length_idx][1], 3.);
        kurt += pow((0. - means[length_idx][0]) / vars[length_idx][1], 4.);
      } else
        nan_count++;
    }
    return ret;
  }

  s::vector<s::array<double, 3>> linear_regress_trade_price_multi_k(const s::array<s::vector<Trade>, HIST_SIZE>& trades, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<s::array<double, 3>> ret; ret.reserve(lengths.size());
    s::vector<s::pair<double, double>> xys;
    uint64 length_idx = 0;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; ++i, cur_idx = prev_idx(cur_idx)){
      if (i == lengths[length_idx]){
        s::sort(s::begin(xys), s::end(xys), [](const s::pair<double,double>& a, const s::pair<double,double>& b){
            return a.first < b.first;
        });
        s::array<double, 3> lg = linear_regression(xys);
        ret.push_back(lg);
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      for (uint64 j = 0; j < trades[cur_idx].size(); ++j)
        xys.push_back(s::make_pair(trades[cur_idx][j].trade_time, trades[cur_idx][j].price));
    }
    return ret;
  }

  s::vector<uint64>           mBookLengths;
  s::vector<uint64>           mTradeLengths;
  s::vector<uint64>           mReturnLengths;
  s::array<uint64, HIST_SIZE> mTimestamps;
  s::vector<s::string>        mPids;
  uint64                      mNxtIdx;
};

struct ETLS1State : public ETLState {
  ETLS1State() : ETLState() {}

  void set_pids(s::vector<s::string>& pids){
    mPids = pids;
    for (s::string& pid : pids)
      mPidDataMap[pid] = PidHistDataS1();
  }

  void insert(uint64 timestamp, const s::map<s::string, s::string>& pid_book, const s::map<s::string, s::string>& pid_trade){
    uint64 pidx = prev_idx(mNxtIdx);
    mTimestamps[mNxtIdx] = timestamp;
    for (const s::string& pid : mPids){
      PidHistDataS1& pid_data = mPidDataMap[pid];

      // process book
      do {
        decltype(pid_book.begin()) it = pid_book.find(pid);
        if (it == pid_book.end()) break;
        j::padded_string book_json((*it).second);
        j::ondemand::document book_data;
        auto error = json_parser.iterate(book_json).get(book_data);
        if (error){
          s::cerr << f::format("ERROR: Failed to read book data from string {}. timestamp {}", (*it).second, timestamp) << s::endl;
          break;
        }

        j::ondemand::array bids;
        error = book_data["bids"].get(bids);
        if (error){
          s::cerr << f::format("ERROR: Failed to get bids from book. timestamp {}", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::value bid : bids){
          j::ondemand::array bid_array; error = bid.get(bid_array);
          if (error){
            s::cerr << f::format("ERROR: Failed to get array from bid array. timestamp {}", timestamp) << s::endl;
            break;
          }
          size_t count = bid_array.count_elements();
          if (count != 3){
            s::cerr << f::format("ERROR: Unexpected number of elements in bid report. count {} timestamp {}", count, timestamp) << s::endl;
            break;
          }
          double price = FNAN, size = FNAN; uint64 hand = 0, i = 0;
          for (j::ondemand::value value : bid_array){
            switch (i){
            break; case 0: parse_double(price, value);
            break; case 1: parse_double(size, value);
            break; case 2: parse_uint(hand, value);
            break; default:
              s::cout << f::format("Extra element in bid array detected, ignored.") << s::endl;
            }
            i++;
          }
          pid_data.mBookBidPrice[mNxtIdx] = price;
          pid_data.mBookBidSize[mNxtIdx]  = size;
          pid_data.mBookBidHand[mNxtIdx]  = hand;
          break;
        }

        j::ondemand::array asks;
        error = book_data["asks"].get(asks);
        if (error){
          s::cerr << f::format("ERROR: Failed to get asks from book. timestamp {}", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::value ask : asks){
          j::ondemand::array ask_array; error = ask.get(ask_array);
          if (error){
            s::cerr << f::format("ERROR: Failed to get array from ask array. timestamp {}", timestamp) << s::endl;
            break;
          }
          size_t count = ask_array.count_elements();
          if (count != 3){
            s::cerr << f::format("ERROR: Unexpected number of elements in ask report. count {} timestamp {}", count, timestamp) << s::endl;
            break;
          }
          double price = FNAN, size = FNAN; uint64 hand = 0, i = 0;
          for (j::ondemand::value value : ask_array){
            switch (i){
            break; case 0: parse_double(price, value);
            break; case 1: parse_double(size, value);
            break; case 2: parse_uint(hand, value);
            break; default:
              s::cout << f::format("Extra element in ask array detected. ignored.") << s::endl;
            }
            i++;
          }
          pid_data.mBookAskPrice[mNxtIdx] = price;
          pid_data.mBookAskSize[mNxtIdx] = size;
          pid_data.mBookAskHand[mNxtIdx] = hand;
          break;
        }

        pid_data.mBookBidAskImbalance[mNxtIdx] = compute_book_imbalance(
            pid_data.mBookBidPrice[mNxtIdx], pid_data.mBookBidSize[mNxtIdx], pid_data.mBookAskPrice[mNxtIdx], pid_data.mBookAskSize[mNxtIdx],
            pid_data.mBookBidPrice[pidx], pid_data.mBookBidSize[pidx], pid_data.mBookAskPrice[pidx], pid_data.mBookAskSize[pidx]
        );

        pid_data.mWapPrice[mNxtIdx] = compute_weighted_average_price(
            pid_data.mBookBidPrice[mNxtIdx], pid_data.mBookBidSize[mNxtIdx], pid_data.mBookAskPrice[mNxtIdx], pid_data.mBookAskSize[mNxtIdx]
        );

        pid_data.mBookBidAskSpread[mNxtIdx] = compute_bidask_spread(pid_data.mBookBidPrice[mNxtIdx], pid_data.mBookAskPrice[mNxtIdx]);
      } while (false);

      // process trade
      do {
        decltype(pid_trade.begin()) it = pid_trade.find(pid);
        if (it == pid_trade.end()) break;

        double total_size = 0., total_volume = 0.;
        uint64 count_buys = 0, count_sells = 0;
        s::vector<Trade> trades;

        j::padded_string trade_json((*it).second);
        j::ondemand::document trade_data;
        auto error = json_parser.iterate(trade_json).get(trade_data);
        if (error){
          s::cerr << f::format("ERROR: Failed to parse trade json. data {} timestamp {}", (*it).second, timestamp) << s::endl;
          break;
        }
        j::ondemand::array trade_array;
        error = trade_data.get_array().get(trade_array);
        if (error){
          s::cerr << f::format("ERROR: Failed to read trade array. timestamp {}", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::object obj : trade_array){
          double trade_price = FNAN, trade_size = FNAN, trade_time = 0.;
          s::string_view trade_side, trade_time_str;
          uint64 trade_id = 0;
          TradeSide ts = TradeSide::INVALID;

          parse_uint(trade_id, obj["trade_id"]);
          parse_string(trade_time_str, obj["time"]);
          parse_double(trade_price, obj["price"]);
          parse_double(trade_size, obj["size"]);
          parse_string(trade_side, obj["side"]);

          trade_time = trade_timestamp_to_epoch(trade_time_str);

          total_size += trade_size;
          total_volume += trade_size * trade_price;
          if (trade_side == "buy"){
            count_buys += 1;
            ts = TradeSide::BUY;
          } else if (trade_side == "sell"){
            count_sells += 1;
            ts = TradeSide::SELL;
          } else
            s::cerr << f::format("ERROR: Unexpected trade side value: {} timestam[ {}", trade_side, timestamp) << s::endl;

          trades.push_back(Trade(trade_id, trade_time, trade_price, trade_size, ts));
        }

        s::sort(s::begin(trades), s::end(trades), [](Trade& a, Trade& b){ return a.trade_id < b.trade_id; });

        pid_data.mTrades[mNxtIdx] = trades;
        pid_data.mTradeNBuys[mNxtIdx]  = count_buys;
        pid_data.mTradeNSells[mNxtIdx] = count_sells;
        pid_data.mTradeBuySellDiff[mNxtIdx] = (double)count_buys - (double)count_sells;
        pid_data.mTradeSize[mNxtIdx]   = total_size;
        pid_data.mTradeVolume[mNxtIdx] = total_volume;
        if (total_size > 0){
          pid_data.mTradeAvgPrice[mNxtIdx] = total_volume / total_size;
          pid_data.mTradeReturn[mNxtIdx] = FNAN;
          uint64 idx = pidx;
          while (idx != mNxtIdx){
            if (mTimestamps[idx] == 0 or mTimestamps[idx] > mTimestamps[mNxtIdx])
              break;
            if (not isnan(pid_data.mTradeAvgPrice[idx])){
              pid_data.mTradeReturn[mNxtIdx] = compute_return(pid_data.mTradeAvgPrice[idx], pid_data.mTradeAvgPrice[mNxtIdx]);
              break;
            }

            idx = prev_idx(idx);
          }
        } else {
          pid_data.mTradeAvgPrice[mNxtIdx] = FNAN;
          pid_data.mTradeReturn[mNxtIdx] = FNAN;
        }
      } while (false);
    }
    mBookMeanReturn27[mNxtIdx] = rolling_mean_return(mNxtIdx, timestamp, 27);

    mNxtIdx = next_idx(mNxtIdx);
  }

  s::unordered_map<s::string, SimpleJsonType> produce_output(uint64 timestamp){
    s::unordered_map<s::string, SimpleJsonType> data;
    data[STIME_COLNAME] = SimpleJsonType(epoch_to_timestamp(timestamp, OUTPUT_DATETIME_FORMAT));
    //TODO: do binary search
    uint64 idx = 0;
    for (; idx < HIST_SIZE; ++idx){
      if (mTimestamps[idx] == timestamp)
        break;
    }
    if (idx >= HIST_SIZE) return data;
    for (const s::string& pid : mPids){
      PidHistDataS1& pid_data = mPidDataMap[pid];

      data[field_name(pid, "best_bid_price")] = pid_data.mBookBidPrice[idx];
      data[field_name(pid, "best_ask_price")] = pid_data.mBookAskPrice[idx];
      data[field_name(pid, "best_bid_size")]  = pid_data.mBookBidSize[idx];
      data[field_name(pid, "best_ask_size")]  = pid_data.mBookAskSize[idx];
      data[field_name(pid, "best_bid_hand")]  = pid_data.mBookBidHand[idx];
      data[field_name(pid, "best_ask_hand")]  = pid_data.mBookAskHand[idx];
      data[field_name(pid, "ba_imbalance")]   = pid_data.mBookBidAskImbalance[idx];
      data[field_name(pid, "wap")]            = pid_data.mWapPrice[idx];
      data[field_name(pid, "book_return")]    = pid_data.mBookReturn[idx];
      data[field_name(pid, "ba_spread")]      = pid_data.mBookBidAskSpread[idx];

      data[field_name(pid, "trade_buy_count")]  =    pid_data.mTradeNBuys[idx];
      data[field_name(pid, "trade_sell_count")] =    pid_data.mTradeNSells[idx];
      data[field_name(pid, "trade_buy_sell_diff")] = pid_data.mTradeBuySellDiff[idx];
      data[field_name(pid, "trade_volume")]     =    pid_data.mTradeVolume[idx];
      data[field_name(pid, "trade_size")]       =    pid_data.mTradeSize[idx];
      data[field_name(pid, "trade_avg_price")]  =    pid_data.mTradeAvgPrice[idx];
      data[field_name(pid, "trade_return")]     =    pid_data.mTradeReturn[idx];

      produce_book_output_rolling_multi_k(data, pid, idx, timestamp);
      produce_trade_output_rolling_multi_k(data, pid, idx, timestamp);

      s::vector<double> book_volatility = rolling_volatility_multi_k(pid_data.mBookReturn, idx, timestamp, mReturnLengths);
      for (uint64 i = 0; i < book_volatility.size(); ++i)
        data[field_name(pid, "book_volatility", mReturnLengths[i], "")] = book_volatility[i];

      data[field_name(pid, "book_beta", 648, "")] = rolling_beta(pid_data.mBookReturn, idx, timestamp, 648);

      s::vector<double> trade_volatility = rolling_volatility_multi_k(pid_data.mTradeReturn, idx, timestamp, mReturnLengths);
      for (uint64 i = 0; i < trade_volatility.size(); ++i)
        data[field_name(pid, "trade_volatility", mReturnLengths[i], "")] = trade_volatility[i];
    }

    data["book_mean_return_27"] = mBookMeanReturn27[idx];

    return data;
  }
protected:
  void produce_book_output_rolling_multi_k(s::unordered_map<s::string, SimpleJsonType>& data, const s::string& pid, uint64 idx, uint64 timestamp){
    PidHistDataS1& pid_data = mPidDataMap[pid];
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidPrice, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_price", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "best_bid_price", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "best_bid_price", mBookLengths[i], "min")] = means[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskPrice, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_price", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "best_ask_price", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "best_ask_price", mBookLengths[i], "min")] = means[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidSize, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_size", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "best_bid_size", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "best_bid_size", mBookLengths[i], "min")] = means[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskSize, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_size", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "best_ask_size", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "best_ask_size", mBookLengths[i], "min")] = means[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidHand, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskHand, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAskImbalance, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidAskImbalance, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookBidAskImbalance, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "min")] = means[i][3];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mWapPrice, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mWapPrice, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mWapPrice, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "wap", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "wap", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "wap", mBookLengths[i], "min")] = means[i][3];
        data[field_name(pid, "wap", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "wap", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "wap", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookReturn, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookReturn, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookReturn, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "book_return", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "book_return", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "book_return", mBookLengths[i], "min")] = means[i][3];
        data[field_name(pid, "book_return", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "book_return", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "book_return", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAskSpread, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ba_spread", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "ba_spread", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "ba_spread", mBookLengths[i], "min")] = output[i][3];
      }
    }
  }

  void produce_trade_output_rolling_multi_k(s::unordered_map<s::string, SimpleJsonType>& data, const s::string& pid, uint64 idx, uint64 timestamp){
    PidHistDataS1& pid_data = mPidDataMap[pid];
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeNBuys, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i)
        data[field_name(pid, "trade_buy_count", mTradeLengths[i], "sum")] = output[i][1];
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeNSells, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i)
        data[field_name(pid, "trade_sell_count", mTradeLengths[i], "sum")] = output[i][1];
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeBuySellDiff, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeBuySellDiff, means, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeBuySellDiff, means, vars, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeSize, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeSize, means, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeSize, means, vars, idx, timestamp, mTradeLengths, true);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_size", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_size", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_size", mTradeLengths[i], "max")] = means[i][2];
        data[field_name(pid, "trade_size", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_size", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_size", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeVolume, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeVolume, means, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeVolume, means, vars, idx, timestamp, mTradeLengths, true);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_volume", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "max")] = means[i][2];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeAvgPrice, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeAvgPrice, means, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeAvgPrice, means, vars, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "max")] = means[i][2];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "min")] = means[i][3];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeReturn, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_return", mTradeLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "trade_return", mTradeLengths[i], "sum")] = output[i][1];
        data[field_name(pid, "trade_return", mTradeLengths[i], "max")] = output[i][2];
        data[field_name(pid, "trade_return", mTradeLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 3>> output = linear_regress_trade_price_multi_k(pid_data.mTrades, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_price_slope", mTradeLengths[i], "")] = output[i][1];
        data[field_name(pid, "trade_price_rmse", mTradeLengths[i], "")]  = output[i][2];
      }
    }

  }

  double rolling_mean_return(uint64 idx, uint64 timestamp, uint64 length){
    double sum = 0.;
    for (const s::string& pid : mPids){
      PidHistDataS1& pid_data = mPidDataMap[pid];
      s::array<double, 2> as = rolling_avg_sum(pid_data.mBookReturn, idx, timestamp, length);
      sum += as[1];
    }
    return sum / mPids.size();
  }

  double rolling_beta(const s::array<double, HIST_SIZE>& prdata, uint64 idx, uint64 timestamp, uint64 length){
    double m2sum = 0., mrsum = 0.;
    uint64 count = 0, nan_count = 0;
    uint64 min_timestamp = timestamp - length;
    for (uint64 i = 0, cur_idx = idx; i < length; i++, cur_idx = prev_idx(cur_idx), count++){
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp)
        continue;
      if (not isnan(mBookMeanReturn27[cur_idx]) and not isnan(prdata[cur_idx])){
        m2sum += pow(mBookMeanReturn27[cur_idx], 2.);
        mrsum += mBookMeanReturn27[cur_idx] * prdata[cur_idx];
      } else
        nan_count++;
    }
    if (nan_count == count) return FNAN;
    else                    return (mrsum / (count - nan_count)) / (m2sum / (count - nan_count));
  }

  template <typename T>
  s::vector<double> rolling_volatility_multi_k(const s::array<T, HIST_SIZE>& data, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<double> ret; ret.reserve(lengths.size());
    double sum = 0.;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    uint64 length_idx = 0;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; i++, cur_idx = prev_idx(cur_idx)){
      if (i == lengths[length_idx]){
        ret.push_back(sum);
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx]))
        sum += pow(data[cur_idx], 2);
    }
    for (uint64 i = ret.size(); i < lengths.size(); ++i)
      ret.push_back(FNAN);
    for (double& s : ret)
      s = sqrt(s);
    return ret;
  }

private:
  s::unordered_map<s::string, PidHistDataS1> mPidDataMap;
  s::array<double, HIST_SIZE>                mBookMeanReturn27;
};

struct ETLS2State : public ETLState {
  ETLS2State() : ETLState() {}

  void set_pids(s::vector<s::string>& pids){
    mPids = pids;
    for (s::string& pid : pids)
      mPidDataMap[pid] = PidHistDataS2();
  }

  void insert(uint64 timestamp, const s::map<s::string, s::string>& pid_book, const s::map<s::string, s::string>& pid_trade){
    uint64 pidx = prev_idx(mNxtIdx);
    mTimestamps[mNxtIdx] = timestamp;
    for (const s::string& pid : mPids){
      PidHistDataS2& pid_data = mPidDataMap[pid];

      // process book
      do {
        decltype(pid_book.begin()) it = pid_book.find(pid);
        if (it == pid_book.end()) break;

        j::padded_string book_json((*it).second);
        j::ondemand::document book_data;
        auto error = json_parser.iterate(book_json).get(book_data);
        if (error) {
          s::cerr << f::format("ERROR: Failed to read book data from string {}. timestamp {}", (*it).second, timestamp) << s::endl;
          break;
        }
        s::vector<double> bid_prices;
        s::vector<double> bid_sizes;
        s::vector<uint64> bid_hands;
        uint64 bid_stack_size = 0;
        j::ondemand::array bids;
        error = book_data["bids"].get(bids);
        if (error) {
          s::cerr << f::format("ERROR: Failed to get bids from book. timestamp {}.", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::value val : bids){
          j::ondemand::json_type ty; error = val.type().get(ty);
          if (error) { continue; }
          switch (ty){
          break; case j::ondemand::json_type::array: {
            j::ondemand::array bid; error = val.get(bid);
            if (error){
              s::cerr << f::format("ERROR: Failed to get array from bid array. timestamp {}", timestamp) << s::endl;
              continue;
            }
            size_t count = bid.count_elements();
            if (count != 3){
              s::cerr << f::format("ERROR: Unexpected number of elements in bid report. count {} timestamp {}", count, timestamp) << s::endl;
              continue;
            }
            double price = FNAN, size = FNAN; uint64 hands = 0, i = 0;
            for (j::ondemand::value value : bid){
              switch (i){
              break; case 0: parse_double(price, value);
              break; case 1: parse_double(size, value);
              break; case 2: parse_uint(hands, value);
              break; default:
                s::cout << f::format("Extra element in bid array detected. ignored.") << s::endl;
              }
              i++;
            }
            bid_prices.push_back(price);
            bid_sizes.push_back(size);
            bid_hands.push_back(hands);
          }
          break; case j::ondemand::json_type::number: {
            j::ondemand::number number = val.get_number();
            bid_stack_size = number.get_uint64();
          }
          break; default:
            s::cerr << f::format("ERROR: Unexpected type in bids data. timestamp {}", timestamp) << s::endl;
            continue;
          }
        }
        pid_data.mBookBidPrices[mNxtIdx]    = bid_prices;
        pid_data.mBookBidSizes[mNxtIdx]     = bid_sizes;
        pid_data.mBookBidHands[mNxtIdx]     = bid_hands;
        pid_data.mBookBidStackSize[mNxtIdx] = bid_stack_size;

        s::vector<double> ask_prices;
        s::vector<double> ask_sizes;
        s::vector<uint64> ask_hands;
        uint64 ask_stack_size = 0;
        j::ondemand::array asks;
        error = book_data["asks"].get(asks);
        if (error) {
          s::cerr << f::format("ERROR: Failed to get asks from book. timestamp {}", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::value val : asks){
          j::ondemand::json_type ty; error = val.type().get(ty);
          if (error) { continue; }
          switch (ty){
          break; case j::ondemand::json_type::array: {
            j::ondemand::array ask; error = val.get(ask);
            if (error){
              s::cerr << f::format("ERROR: Failed to get array from ask array. timestamp {}", timestamp) << s::endl;
              continue;
            }
            size_t count = ask.count_elements();
            if (count != 3){
              s::cerr << f::format("ERROR: Unexpected number of elements in ask report. count {} timestamp {}", count, timestamp) << s::endl;
              continue;
            }
            double price = FNAN, size = FNAN; uint64 hands = 0, i = 0;
            for (j::ondemand::value value : ask){
              switch (i){
              break; case 0: parse_double(price, value);
              break; case 1: parse_double(size, value);
              break; case 2: parse_uint(hands, value);
              break; default:
                s::cout << f::format("Extra element in ask array detected. ignored") << s::endl;
              }
              i++;
            }
            ask_prices.push_back(price);
            ask_sizes.push_back(size);
            ask_hands.push_back(hands);
          }
          break; case j::ondemand::json_type::number: {
            j::ondemand::number number = val.get_number();
            ask_stack_size = number.get_uint64();
          }
          break; default:
            s::cerr << f::format("ERROR: Unexpected type in ask array. timestamp {}", timestamp) << s::endl;
            continue;
          }
        }

        pid_data.mBookAskPrices[mNxtIdx]    = ask_prices;
        pid_data.mBookAskSizes[mNxtIdx]     = ask_sizes;
        pid_data.mBookAskHands[mNxtIdx]     = ask_hands;
        pid_data.mBookAskStackSize[mNxtIdx] = ask_stack_size;

        pid_data.mBookBidTick1[mNxtIdx] = compute_book_first_tick(pid_data.mBookBidPrices[mNxtIdx]);
        pid_data.mBookAskTick1[mNxtIdx] = compute_book_first_tick(pid_data.mBookAskPrices[mNxtIdx]);
        pid_data.mBookBidAvgTick[mNxtIdx] = compute_book_avg_tick(pid_data.mBookBidPrices[mNxtIdx]);
        pid_data.mBookAskAvgTick[mNxtIdx] = compute_book_avg_tick(pid_data.mBookAskPrices[mNxtIdx]);

        pid_data.mBookBidSizeChange[mNxtIdx] = compute_bid_size_change(
          pid_data.mBookBidPrices[mNxtIdx], pid_data.mBookBidSizes[mNxtIdx], pid_data.mBookBidPrices[pidx], pid_data.mBookBidSizes[pidx]
        );
        pid_data.mBookAskSizeChange[mNxtIdx] = compute_ask_size_change(
          pid_data.mBookAskPrices[mNxtIdx], pid_data.mBookAskSizes[mNxtIdx], pid_data.mBookAskPrices[pidx], pid_data.mBookAskSizes[pidx]
        );
        pid_data.mBookBidVolumeChange[mNxtIdx] = compute_bid_volume_change(
          pid_data.mBookBidPrices[mNxtIdx], pid_data.mBookBidSizes[mNxtIdx], pid_data.mBookBidPrices[pidx], pid_data.mBookBidSizes[pidx]
        );
        pid_data.mBookAskVolumeChange[mNxtIdx] = compute_ask_volume_change(
          pid_data.mBookAskPrices[mNxtIdx], pid_data.mBookAskSizes[mNxtIdx], pid_data.mBookAskPrices[pidx], pid_data.mBookAskSizes[pidx]
        );

        s::array<double, 3> bid_lg = linear_regression(pid_data.mBookBidPrices[mNxtIdx], pid_data.mBookBidSizes[mNxtIdx]);
        pid_data.mBookBidLevelIntercept[mNxtIdx] = bid_lg[0];
        pid_data.mBookBidLevelSlope[mNxtIdx] = bid_lg[1];
        pid_data.mBookBidLevelRMSE[mNxtIdx] = bid_lg[2];
        s::array<double, 3> ask_lg = linear_regression(pid_data.mBookAskPrices[mNxtIdx], pid_data.mBookAskSizes[mNxtIdx]);
        pid_data.mBookAskLevelIntercept[mNxtIdx] = ask_lg[0];
        pid_data.mBookAskLevelSlope[mNxtIdx] = ask_lg[1];
        pid_data.mBookAskLevelRMSE[mNxtIdx] = ask_lg[2];

        if (pid_data.mBookBidPrices[mNxtIdx].size() > 0 and pid_data.mBookBidSizes[mNxtIdx].size() > 0 and
            pid_data.mBookAskPrices[mNxtIdx].size() > 0 and pid_data.mBookAskSizes[mNxtIdx].size() > 0 and
            pid_data.mBookBidPrices[pidx].size() > 0 and pid_data.mBookBidSizes[pidx].size() > 0 and
            pid_data.mBookAskPrices[pidx].size() > 0 and pid_data.mBookAskSizes[pidx].size() > 0)
          pid_data.mBookBidAskImbalance[mNxtIdx] = compute_book_imbalance(
            pid_data.mBookBidPrices[mNxtIdx][0], pid_data.mBookBidSizes[mNxtIdx][0], pid_data.mBookAskPrices[mNxtIdx][0], pid_data.mBookAskSizes[mNxtIdx][0],
            pid_data.mBookBidPrices[pidx][0],    pid_data.mBookBidSizes[pidx][0],    pid_data.mBookAskPrices[pidx][0],    pid_data.mBookAskSizes[pidx][0]
          );
        else
          pid_data.mBookBidAskImbalance[mNxtIdx] = FNAN;

        pid_data.mBookBidAskImbalanceExtended[mNxtIdx] = compute_extended_book_imbalance(
            pid_data.mBookBidPrices[mNxtIdx], pid_data.mBookBidSizes[mNxtIdx], pid_data.mBookAskPrices[mNxtIdx], pid_data.mBookAskSizes[mNxtIdx],
            pid_data.mBookBidPrices[pidx],    pid_data.mBookBidSizes[pidx],    pid_data.mBookAskPrices[pidx],    pid_data.mBookAskSizes[pidx]
        );

        if (pid_data.mBookBidPrices[mNxtIdx].size() > 0 and pid_data.mBookBidSizes[mNxtIdx].size() > 0 and
            pid_data.mBookAskPrices[mNxtIdx].size() > 0 and pid_data.mBookAskSizes[mNxtIdx].size() > 0)
          pid_data.mWapPrice[mNxtIdx] = compute_weighted_average_price(
            pid_data.mBookBidPrices[mNxtIdx][0], pid_data.mBookBidSizes[mNxtIdx][0], pid_data.mBookAskPrices[mNxtIdx][0], pid_data.mBookAskSizes[mNxtIdx][0]
          );
        else
          pid_data.mWapPrice[mNxtIdx] = FNAN;
        pid_data.mBookReturn[mNxtIdx] = compute_return(pid_data.mWapPrice[pidx], pid_data.mWapPrice[mNxtIdx]);

        if (pid_data.mBookBidPrices[mNxtIdx].size() > 0 and pid_data.mBookAskPrices[mNxtIdx].size() > 0)
          pid_data.mBookBidAskSpread[mNxtIdx] = compute_bidask_spread(
            pid_data.mBookBidPrices[mNxtIdx][0], pid_data.mBookAskPrices[mNxtIdx][0]
          );
        else
          pid_data.mBookBidAskSpread[mNxtIdx] = FNAN;

      } while (false);

      // process trade
      do {
        decltype(pid_trade.begin()) it = pid_trade.find(pid);
        if (it == pid_trade.end()) break;

        double total_size = 0., total_volume = 0.;
        uint64 count_buys = 0, count_sells = 0;
        s::vector<Trade> trades;

        j::padded_string trade_json((*it).second);
        j::ondemand::document trade_data;
        auto error = json_parser.iterate(trade_json).get(trade_data);
        if (error) {
          s::cerr << f::format("ERROR: Failed to parse trade json. data {} timestamp {}", (*it).second, timestamp) << s::endl;
          break;
        }
        j::ondemand::array trade_array;
        error = trade_data.get_array().get(trade_array);
        if (error) {
          s::cerr << f::format("ERROR: Failed to read trade array. timestamp {}", timestamp) << s::endl;
          break;
        }
        for (j::ondemand::object obj : trade_array){
          double trade_price = FNAN, trade_size = FNAN, trade_time = 0.;
          s::string_view trade_side, trade_time_str;
          TradeSide ts = TradeSide::INVALID;
          uint64 trade_id;

          //TODO: trade_price and trade_size can be FNAN
          parse_uint(trade_id, obj["trade_id"]);
          parse_string(trade_time_str, obj["time"]);
          parse_double(trade_price, obj["price"]);
          parse_double(trade_size, obj["size"]);
          parse_string(trade_side, obj["side"]);

          trade_time = trade_timestamp_to_epoch(trade_time_str);

          total_size += trade_size;
          total_volume += trade_size * trade_price;
          if (trade_side == "buy"){
            count_buys += 1;
            ts = TradeSide::BUY;
          } else if (trade_side == "sell"){
            count_sells += 1;
            ts = TradeSide::SELL;
          } else
            s::cerr << f::format("ERROR: Unexpected trade side value: {} timestamp {}", trade_side, timestamp) << s::endl;

          trades.push_back(Trade(trade_id, trade_time, trade_price, trade_size, ts));
        }

        s::sort(s::begin(trades), s::end(trades), [](Trade& a, Trade& b){ return a.trade_id < b.trade_id; });

        pid_data.mTrades[mNxtIdx] = trades;
        pid_data.mTradeNBuys[mNxtIdx] = count_buys;
        pid_data.mTradeNSells[mNxtIdx] = count_sells;
        pid_data.mTradeBuySellDiff[mNxtIdx] = (double)count_buys - (double)count_sells;
        pid_data.mTradeSize[mNxtIdx] = total_size;
        pid_data.mTradeVolume[mNxtIdx] = total_volume;
        if (total_size > 0){
          pid_data.mTradeAvgPrice[mNxtIdx] = total_volume / total_size;
          pid_data.mTradeReturn[mNxtIdx] = FNAN;
          uint64 idx = pidx;
          while (idx != mNxtIdx){
            if (mTimestamps[idx] == 0 or mTimestamps[idx] > mTimestamps[mNxtIdx])
              break;
            if (not isnan(pid_data.mTradeAvgPrice[idx])){
              pid_data.mTradeReturn[mNxtIdx] = compute_return(pid_data.mTradeAvgPrice[idx], pid_data.mTradeAvgPrice[mNxtIdx]);
              break;
            }

            idx = prev_idx(idx);
          }
        } else {
          pid_data.mTradeAvgPrice[mNxtIdx] = FNAN;
          pid_data.mTradeReturn[mNxtIdx] = FNAN;
        }
      } while (false);
    }
    mBookMeanReturn27[mNxtIdx] = rolling_mean_return(mNxtIdx, timestamp, 27);

    mNxtIdx = next_idx(mNxtIdx);
  }

  s::unordered_map<s::string, SimpleJsonType> produce_output(uint64 timestamp){
    s::unordered_map<s::string, SimpleJsonType> data;
    data[STIME_COLNAME] = SimpleJsonType(epoch_to_timestamp(timestamp, OUTPUT_DATETIME_FORMAT));
    //TODO: do binary search
    uint64 idx = 0;
    for (; idx < HIST_SIZE; ++idx)
      if (mTimestamps[idx] == timestamp)
        break;
    if (idx >= HIST_SIZE) return data;
    for (const s::string& pid : mPids){
      PidHistDataS2& pid_data = mPidDataMap[pid];

      if (pid_data.mBookBidPrices[idx].size() > 0) data[field_name(pid, "best_bid_price")] = pid_data.mBookBidPrices[idx][0];
      else                                         data[field_name(pid, "best_bid_price")] = FNAN;
      if (pid_data.mBookAskPrices[idx].size() > 0) data[field_name(pid, "best_ask_price")] = pid_data.mBookAskPrices[idx][0];
      else                                         data[field_name(pid, "best_ask_price")] = FNAN;
      if (pid_data.mBookBidSizes[idx].size() > 0)  data[field_name(pid, "best_bid_size")] = pid_data.mBookBidSizes[idx][0];
      else                                         data[field_name(pid, "best_bid_size")] = FNAN;
      if (pid_data.mBookAskSizes[idx].size() > 0)  data[field_name(pid, "best_ask_size")] = pid_data.mBookAskSizes[idx][0];
      else                                         data[field_name(pid, "best_ask_size")] = FNAN;
      if (pid_data.mBookBidHands[idx].size() > 0)  data[field_name(pid, "best_bid_hand")] = pid_data.mBookBidHands[idx][0];
      else                                         data[field_name(pid, "best_bid_hand")] = FNAN;
      if (pid_data.mBookAskHands[idx].size() > 0)  data[field_name(pid, "best_ask_hand")] = pid_data.mBookAskHands[idx][0];
      else                                         data[field_name(pid, "best_ask_hand")] = FNAN;

      if (pid_data.mBookBidPrices[idx].size() > 1) data[field_name(pid, "bid_price2")] = pid_data.mBookBidPrices[idx][1];
      else                                         data[field_name(pid, "bid_price2")] = FNAN;
      if (pid_data.mBookAskPrices[idx].size() > 1) data[field_name(pid, "ask_price2")] = pid_data.mBookAskPrices[idx][1];
      else                                         data[field_name(pid, "ask_price2")] = FNAN;
      if (pid_data.mBookBidSizes[idx].size() > 1)  data[field_name(pid, "bid_size2")]  = pid_data.mBookBidSizes[idx][1];
      else                                         data[field_name(pid, "bid_size2")]  = FNAN;
      if (pid_data.mBookAskSizes[idx].size() > 1)  data[field_name(pid, "ask_size2")]  = pid_data.mBookAskSizes[idx][1];
      else                                         data[field_name(pid, "ask_size2")]  = FNAN;
      if (pid_data.mBookBidHands[idx].size() > 1)  data[field_name(pid, "bid_hand2")]  = pid_data.mBookBidHands[idx][1];
      else                                         data[field_name(pid, "bid_hand2")]  = FNAN;
      if (pid_data.mBookAskHands[idx].size() > 1)  data[field_name(pid, "ask_hand2")]  = pid_data.mBookAskHands[idx][1];
      else                                         data[field_name(pid, "ask_hand2")]  = FNAN;

      if (pid_data.mBookBidPrices[idx].size() > 2) data[field_name(pid, "bid_price3")] = pid_data.mBookBidPrices[idx][2];
      else                                         data[field_name(pid, "bid_price3")] = FNAN;
      if (pid_data.mBookAskPrices[idx].size() > 2) data[field_name(pid, "ask_price3")] = pid_data.mBookAskPrices[idx][2];
      else                                         data[field_name(pid, "ask_price3")] = FNAN;
      if (pid_data.mBookBidSizes[idx].size() > 2)  data[field_name(pid, "bid_size3")]  = pid_data.mBookBidSizes[idx][2];
      else                                         data[field_name(pid, "bid_size3")]  = FNAN;
      if (pid_data.mBookAskSizes[idx].size() > 2)  data[field_name(pid, "ask_size3")]  = pid_data.mBookAskSizes[idx][2];
      else                                         data[field_name(pid, "ask_size3")]  = FNAN;
      if (pid_data.mBookBidHands[idx].size() > 2)  data[field_name(pid, "bid_hand3")]  = pid_data.mBookBidHands[idx][2];
      else                                         data[field_name(pid, "bid_hand3")]  = FNAN;
      if (pid_data.mBookAskHands[idx].size() > 2)  data[field_name(pid, "ask_hand3")]  = pid_data.mBookAskHands[idx][2];
      else                                         data[field_name(pid, "ask_hand3")]  = FNAN;

      data[field_name(pid, "bid_level_size")] =        pid_data.mBookBidStackSize[idx];
      data[field_name(pid, "ask_level_size")] =        pid_data.mBookAskStackSize[idx];
      data[field_name(pid, "bid_tick1")] =             pid_data.mBookBidTick1[idx];
      data[field_name(pid, "ask_tick1")] =             pid_data.mBookAskTick1[idx];
      data[field_name(pid, "bid_avg_tick")] =          pid_data.mBookBidAvgTick[idx];
      data[field_name(pid, "ask_avg_tick")] =          pid_data.mBookAskAvgTick[idx];
      data[field_name(pid, "bid_size_change")] =       pid_data.mBookBidSizeChange[idx];
      data[field_name(pid, "ask_size_change")] =       pid_data.mBookAskSizeChange[idx];
      data[field_name(pid, "bid_volume_change")] =     pid_data.mBookBidVolumeChange[idx];
      data[field_name(pid, "ask_volume_change")] =     pid_data.mBookAskVolumeChange[idx];
      data[field_name(pid, "bid_level_slope")] =       pid_data.mBookBidLevelSlope[idx];
      data[field_name(pid, "ask_level_slope")] =       pid_data.mBookAskLevelSlope[idx];
      data[field_name(pid, "bid_level_intercept")] =   pid_data.mBookBidLevelIntercept[idx];
      data[field_name(pid, "ask_level_intercept")] =   pid_data.mBookAskLevelIntercept[idx];
      data[field_name(pid, "bid_level_rmse")] =        pid_data.mBookBidLevelRMSE[idx];
      data[field_name(pid, "ask_level_rmse")] =        pid_data.mBookAskLevelRMSE[idx];
      data[field_name(pid, "ba_imbalance")] =          pid_data.mBookBidAskImbalance[idx];
      data[field_name(pid, "ba_imbalance_extended")] = pid_data.mBookBidAskImbalanceExtended[idx];
      data[field_name(pid, "wap")] =                   pid_data.mWapPrice[idx];
      data[field_name(pid, "book_return")] =           pid_data.mBookReturn[idx];
      data[field_name(pid, "ba_spread")] =             pid_data.mBookBidAskSpread[idx];

      data[field_name(pid, "trade_buy_count")]  =    pid_data.mTradeNBuys[idx];
      data[field_name(pid, "trade_sell_count")] =    pid_data.mTradeNSells[idx];
      data[field_name(pid, "trade_buy_sell_diff")] = pid_data.mTradeBuySellDiff[idx];
      data[field_name(pid, "trade_volume")]     =    pid_data.mTradeVolume[idx];
      data[field_name(pid, "trade_size")]       =    pid_data.mTradeSize[idx];
      data[field_name(pid, "trade_avg_price")]  =    pid_data.mTradeAvgPrice[idx];
      data[field_name(pid, "trade_return")]     =    pid_data.mTradeReturn[idx];

      produce_book_output_rolling_multi_k(data, pid, idx, timestamp);
      produce_trade_output_rolling_multi_k(data, pid, idx, timestamp);

      s::vector<double> book_volatility = rolling_volatility_multi_k(pid_data.mBookReturn, idx, timestamp, mReturnLengths);
      for (uint64 i = 0; i < book_volatility.size(); ++i)
        data[field_name(pid, "book_volatility", mReturnLengths[i], "")] = book_volatility[i];

      data[field_name(pid, "book_beta", 648, "")] = rolling_beta(pid_data.mBookReturn, idx, timestamp, 648);

      s::vector<double> trade_volatility = rolling_volatility_multi_k(pid_data.mTradeReturn, idx, timestamp, mReturnLengths);
      for (uint64 i = 0; i < trade_volatility.size(); ++i)
        data[field_name(pid, "trade_volatility", mReturnLengths[i], "")] = trade_volatility[i];
    }

    data["book_mean_return_27"] = mBookMeanReturn27[idx];

    return data;
  }

protected:
  void produce_book_output_rolling_multi_k(s::unordered_map<s::string, SimpleJsonType>& data, const s::string& pid, uint64 idx, uint64 timestamp){
    PidHistDataS2& pid_data = mPidDataMap[pid];
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookBidPrices, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_price", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_bid_price", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_bid_price", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookAskPrices, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_price", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_ask_price", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_ask_price", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookBidSizes, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_size", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_bid_size", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_bid_size", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookAskSizes, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_size", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_ask_size", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_ask_size", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookBidHands, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_bid_hand", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_aoa_multi_k(pid_data.mBookAskHands, idx, 0, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "best_ask_hand", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidTick1, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i)
        data[field_name(pid, "bid_tick1", mBookLengths[i], "avg")] = output[i][0];
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskTick1, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i)
        data[field_name(pid, "ask_tick1", mBookLengths[i], "avg")] = output[i][0];
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAvgTick, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidAvgTick, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookBidAvgTick, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_avg_tick", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "bid_avg_tick", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "bid_avg_tick", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "bid_avg_tick", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskAvgTick, idx, timestamp, mBookLengths);
      s::vector<s::array<double,2 >> vars  = rolling_var_std_multi_k(pid_data.mBookAskAvgTick, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double,2 >> skews = rolling_skew_kurt_multi_k(pid_data.mBookAskAvgTick, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_avg_tick", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ask_avg_tick", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "ask_avg_tick", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "ask_avg_tick", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_abs_avg_sum_max_min_multi_k(pid_data.mBookBidSizeChange, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidSizeChange, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_abs_skew_kurt_multi_k(pid_data.mBookBidSizeChange, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_size_change", mBookLengths[i], "max")]    = means[i][2];
        data[field_name(pid, "bid_size_change", mBookLengths[i], "min")]    = means[i][3];
        data[field_name(pid, "bid_size_change", mBookLengths[i], "absavg")] = means[i][0];
        data[field_name(pid, "bid_size_change", mBookLengths[i], "std")]    = vars[i][1];
        data[field_name(pid, "bid_size_change", mBookLengths[i], "skew")]   = skews[i][0];
        data[field_name(pid, "bid_size_change", mBookLengths[i], "kurt")]   = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_abs_avg_sum_max_min_multi_k(pid_data.mBookAskSizeChange, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookAskSizeChange, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_abs_skew_kurt_multi_k(pid_data.mBookAskSizeChange, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_size_change", mBookLengths[i], "max")]    = means[i][2];
        data[field_name(pid, "ask_size_change", mBookLengths[i], "min")]    = means[i][3];
        data[field_name(pid, "ask_size_change", mBookLengths[i], "absavg")] = means[i][0];
        data[field_name(pid, "ask_size_change", mBookLengths[i], "std")]    = vars[i][1];
        data[field_name(pid, "ask_size_change", mBookLengths[i], "skew")]   = skews[i][0];
        data[field_name(pid, "ask_size_change", mBookLengths[i], "kurt")]   = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_abs_avg_sum_max_min_multi_k(pid_data.mBookBidVolumeChange, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidVolumeChange, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_abs_skew_kurt_multi_k(pid_data.mBookBidVolumeChange, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_volume_change", mBookLengths[i], "absavg")] = means[i][0];
        data[field_name(pid, "bid_volume_change", mBookLengths[i], "std")]    = vars[i][1];
        data[field_name(pid, "bid_volume_change", mBookLengths[i], "skew")]   = skews[i][0];
        data[field_name(pid, "bid_volume_change", mBookLengths[i], "kurt")]   = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_abs_avg_sum_max_min_multi_k(pid_data.mBookAskVolumeChange, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookAskVolumeChange, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_abs_skew_kurt_multi_k(pid_data.mBookAskVolumeChange, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_volume_change", mBookLengths[i], "absavg")] = means[i][0];
        data[field_name(pid, "ask_volume_change", mBookLengths[i], "std")]    = vars[i][1];
        data[field_name(pid, "ask_volume_change", mBookLengths[i], "skew")]   = skews[i][0];
        data[field_name(pid, "ask_volume_change", mBookLengths[i], "kurt")]   = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidLevelSlope, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidLevelSlope, means, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_level_slope", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "bid_level_slope", mBookLengths[i], "std")] = vars[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskLevelSlope, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookAskLevelSlope, means, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_level_slope", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ask_level_slope", mBookLengths[i], "std")] = vars[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidLevelIntercept, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidLevelIntercept, means, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_level_intercept", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "bid_level_intercept", mBookLengths[i], "std")] = vars[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskLevelIntercept, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookAskLevelIntercept, means, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_level_intercept", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ask_level_intercept", mBookLengths[i], "std")] = vars[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidLevelRMSE, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "bid_level_rmse", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "bid_level_rmse", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "bid_level_rmse", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookAskLevelRMSE, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ask_level_rmse", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "ask_level_rmse", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "ask_level_rmse", mBookLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAskImbalance, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidAskImbalance, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookBidAskImbalance, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "min")] = means[i][3];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "ba_imbalance", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAskImbalanceExtended, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookBidAskImbalanceExtended, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookBidAskImbalanceExtended, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "max")] = means[i][2];
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "min")] = means[i][3];
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "ba_imbalance_extended", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mWapPrice, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mWapPrice, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mWapPrice, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "wap", mBookLengths[i], "avg")]  = means[i][0];
        data[field_name(pid, "wap", mBookLengths[i], "max")]  = means[i][2];
        data[field_name(pid, "wap", mBookLengths[i], "min")]  = means[i][3];
        data[field_name(pid, "wap", mBookLengths[i], "std")]  = vars[i][1];
        data[field_name(pid, "wap", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "wap", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mBookReturn, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mBookReturn, means, idx, timestamp, mBookLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mBookReturn, means, vars, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "book_return", mBookLengths[i], "avg")]  = means[i][0];
        data[field_name(pid, "book_return", mBookLengths[i], "max")]  = means[i][2];
        data[field_name(pid, "book_return", mBookLengths[i], "min")]  = means[i][3];
        data[field_name(pid, "book_return", mBookLengths[i], "std")]  = vars[i][1];
        data[field_name(pid, "book_return", mBookLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "book_return", mBookLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mBookBidAskSpread, idx, timestamp, mBookLengths);
      for (uint64 i = 0; i < mBookLengths.size(); ++i){
        data[field_name(pid, "ba_spread", mBookLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "ba_spread", mBookLengths[i], "max")] = output[i][2];
        data[field_name(pid, "ba_spread", mBookLengths[i], "min")] = output[i][3];
      }
    }
  }

  void produce_trade_output_rolling_multi_k(s::unordered_map<s::string, SimpleJsonType>& data, const s::string& pid, uint64 idx, uint64 timestamp){
    PidHistDataS2& pid_data = mPidDataMap[pid];
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeNBuys, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i)
        data[field_name(pid, "trade_buy_count", mTradeLengths[i], "sum")] = output[i][1];
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeNSells, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i)
        data[field_name(pid, "trade_sell_count", mTradeLengths[i], "sum")] = output[i][1];
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeBuySellDiff, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeBuySellDiff, means, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeBuySellDiff, means, vars, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_buy_sell_diff", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeSize, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeSize, means, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeSize, means, vars, idx, timestamp, mTradeLengths, true);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_size", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_size", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_size", mTradeLengths[i], "max")] = means[i][2];
        data[field_name(pid, "trade_size", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_size", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_size", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeVolume, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeVolume, means, idx, timestamp, mTradeLengths, true);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeVolume, means, vars, idx, timestamp, mTradeLengths, true);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_volume", mTradeLengths[i], "sum")] = means[i][1];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_volume", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> means = rolling_avg_sum_max_min_multi_k(pid_data.mTradeAvgPrice, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> vars  = rolling_var_std_multi_k(pid_data.mTradeAvgPrice, means, idx, timestamp, mTradeLengths);
      s::vector<s::array<double, 2>> skews = rolling_skew_kurt_multi_k(pid_data.mTradeAvgPrice, means, vars, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "avg")] = means[i][0];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "max")] = means[i][2];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "min")] = means[i][3];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "std")] = vars[i][1];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "skew")] = skews[i][0];
        data[field_name(pid, "trade_avg_price", mTradeLengths[i], "kurt")] = skews[i][1];
      }
    }
    {
      s::vector<s::array<double, 4>> output = rolling_avg_sum_max_min_multi_k(pid_data.mTradeReturn, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_return", mTradeLengths[i], "avg")] = output[i][0];
        data[field_name(pid, "trade_return", mTradeLengths[i], "sum")] = output[i][1];
        data[field_name(pid, "trade_return", mTradeLengths[i], "max")] = output[i][2];
        data[field_name(pid, "trade_return", mTradeLengths[i], "min")] = output[i][3];
      }
    }
    {
      s::vector<s::array<double, 3>> output = linear_regress_trade_price_multi_k(pid_data.mTrades, idx, timestamp, mTradeLengths);
      for (uint64 i = 0; i < mTradeLengths.size(); ++i){
        data[field_name(pid, "trade_price_slope", mTradeLengths[i], "")] = output[i][1];
        data[field_name(pid, "trade_price_rmse", mTradeLengths[i], "")]  = output[i][2];
      }
    }
  }

  double rolling_mean_return(uint64 idx, uint64 timestamp, uint64 length){
    double sum = 0.;
    for (const s::string& pid : mPids){
      PidHistDataS2& pid_data = mPidDataMap[pid];
      s::array<double, 2> as = rolling_avg_sum(pid_data.mBookReturn, idx, timestamp, length);
      sum += as[1];
    }
    return sum / mPids.size();
  }

  double rolling_beta(const s::array<double, HIST_SIZE>& prdata, uint64 idx, uint64 timestamp, uint64 length){
    double m2sum = 0., mrsum = 0.;
    uint64 count = 0, nan_count = 0;
    uint64 min_timestamp = timestamp - length;
    for (uint64 i = 0, cur_idx = idx; i < length; i++, cur_idx = prev_idx(cur_idx), count++){
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp)
        continue;
      if (not isnan(mBookMeanReturn27[cur_idx]) and not isnan(prdata[cur_idx])){
        m2sum += pow(mBookMeanReturn27[cur_idx], 2.);
        mrsum += mBookMeanReturn27[cur_idx] * prdata[cur_idx];
      } else
        nan_count++;
    }
    if (nan_count == count) return FNAN;
    else                    return (mrsum / (count - nan_count)) / (m2sum / (count - nan_count));
  }

  template <typename T>
  s::vector<double> rolling_volatility_multi_k(const s::array<T, HIST_SIZE>& data, uint64 idx, uint64 timestamp, const s::vector<uint64>& lengths){
    s::vector<double> ret; ret.reserve(lengths.size());
    double sum = 0.;
    uint64 max_length = lengths.back();
    uint64 min_timestamp = timestamp - max_length;
    uint64 length_idx = 0;
    for (uint64 i = 0, cur_idx = idx; i < max_length + 1; i++, cur_idx = prev_idx(cur_idx)){
      if (i == lengths[length_idx]){
        ret.push_back(sum);
        length_idx++;
      }
      if (mTimestamps[cur_idx] > timestamp or mTimestamps[cur_idx] <= min_timestamp) continue;
      if (not isnan(data[cur_idx]))
        sum += pow(data[cur_idx], 2);
    }
    for (uint64 i = ret.size(); i < lengths.size(); ++i)
      ret.push_back(FNAN);
    for (double& s : ret)
      s = sqrt(s);
    return ret;
  }

private:
  s::unordered_map<s::string, PidHistDataS2> mPidDataMap;
  s::array<double, HIST_SIZE>                mBookMeanReturn27;
};

PYBIND11_MODULE(cppext, m){
  m.doc() = R"pbdoc(
      Prediction ETL S2 State
      -----------------------
      .. currentmodule:: cppext

      .. autosummary::
         :tctree: _generate
  )pbdoc";

  py::class_<ETLS1State>(m, "ETLS1State")
    .def(py::init<>())
    .def("hist_size", &ETLS1State::hist_size)
    .def("set_pids", &ETLS1State::set_pids)
    .def("insert", &ETLS1State::insert)
    .def("produce_output", &ETLS1State::produce_output)
    ;

  py::class_<ETLS2State>(m, "ETLS2State")
    .def(py::init<>())
    .def("hist_size", &ETLS2State::hist_size)
    .def("set_pids", &ETLS2State::set_pids)
    .def("insert", &ETLS2State::insert)
    .def("produce_output", &ETLS2State::produce_output)
    ;
}
