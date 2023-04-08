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
    cbprices = [100., 99., 98., 97.]
    cbsizes  = [13.,  15., 17., 10.]
    pbprices = [97., 96., 95.]
    pbsizes  = [1.,  100., 200.]

    c = compute_bid_size_change(cbprices, cbsizes, pbprices, pbsizes)
    self.assertAlmostEqual(54., c)

  def test_2(self):
    pbprices = [100., 99., 98., 97.]
    pbsizes  = [13.,  15., 17., 10.]
    cbprices = [97., 96., 95.]
    cbsizes  = [1.,  100., 200.]

    c = compute_bid_size_change(cbprices, cbsizes, pbprices, pbsizes)
    #TODO: shouldn't this be -54 ?
    self.assertAlmostEqual(-55., c)

  def test_3(self):
    cbprices = [100., 99., 98., 97.]
    cbsizes  = [13.,  15., 17., 10.]
    pbprices = [100., 99., 98., 97.]
    pbsizes  = [15.,  17., 19., 13.]

    c = compute_bid_size_change(cbprices, cbsizes, pbprices, pbsizes)
    self.assertAlmostEqual(-2., c)

  def test_4(self):
    cbprices = [100., 99., 98., 97.]
    cbsizes  = [13.,  15., 17., 10.]
    pbprices = [100., 98., 97.]
    pbsizes  = [15.,  19., 13.]

    c = compute_bid_size_change(cbprices, cbsizes, pbprices, pbsizes)
    self.assertAlmostEqual(-2., c)


class TestComputeAskSizeChange(unittest.TestCase):
  def test_1(self):
    caprices = [100., 101., 102., 103.]
    casizes  = [13.,  15.,  17.,  10.]
    paprices = [103., 104., 105.]
    pasizes  = [1.,   100., 200.]

    c = compute_ask_size_change(caprices, casizes, paprices, pasizes)
    self.assertAlmostEqual(54., c)

  def test_2(self):
    paprices = [100., 101., 102., 103.]
    pasizes  = [13.,  15.,  17.,  10.]
    caprices = [103., 104., 105.]
    casizes  = [1.,   100., 200.]

    c = compute_ask_size_change(caprices, casizes, paprices, pasizes)
    #TODO: shouldn't this be -54 ?
    self.assertAlmostEqual(-55., c)

  def test_3(self):
    caprices = [100., 101., 102., 103.]
    casizes  = [13.,  15.,  17.,  10.]
    paprices = [100., 101., 102., 103.]
    pasizes  = [15.,  17.,  19.,  13.]

    c = compute_ask_size_change(caprices, casizes, paprices, pasizes)
    self.assertAlmostEqual(-2., c)

  def test_4(self):
    caprices = [100., 101., 102., 103.]
    casizes  = [13.,  15.,  17.,  10.]
    paprices = [100., 102., 103.]
    pasizes  = [15.,  19.,  13.]

    c = compute_ask_size_change(caprices, casizes, paprices, pasizes)
    self.assertAlmostEqual(-2., c)


if __name__ == '__main__':
  unittest.main()
