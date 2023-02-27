import unittest

from senseis.calculation import compute_book_imbalance
from senseis.calculation import compute_extended_book_imbalance
from senseis.calculation import compute_bid_size_change
from senseis.calculation import compute_ask_size_change
from senseis.calculation import compute_bid_volume_change
from senseis.calculation import compute_ask_volume_change

class TestComputeBookImbalance(unittest.TestCase):
  def test_1(self):
    cbprice = 99.
    cbsize  = 1.
    pbprice = 100.
    pbsize  = 1.2

    caprice = 101.
    casize  = 2.
    paprice = 101.
    pasize  = 4.

    imba = compute_book_imbalance(cbprice, cbsize, caprice, casize, pbprice, pbsize, paprice, pasize)
    self.assertAlmostEqual(0.8, imba)


class TestComputeExtendedBookImbalance(unittest.TestCase):
  def test_1(self):
    cbprices = [99., 98., 97., 96.]
    cbsizes  = [1.,  2.,  3.,  4.]
    pbprices = [100., 99.5, 99., 98.]
    pbsizes  = [1.2,  0.5,  1.,  3.]

    caprices = [101., 102., 103., 104.]
    casizes  = [2.,  3.,  4.,  5.]
    paprices = [101., 102., 103., 104.]
    pasizes  = [4.,   3.,   5.,   9.]

    imba = compute_extended_book_imbalance(cbprices, cbsizes, caprices, casizes, pbprices, pbsizes, paprices, pasizes)
    self.assertAlmostEqual(0.3, imba)

class TestComputeBidSizeChange(unittest.TestCase):
  def test_1(self):
    pass

class TestComputeAskSizeChange(unittest.TestCase):
  def test_1(self):
    pass

if __name__ == '__main__':
  unittest.main()
