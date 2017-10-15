import unittest

from mongo_observer.models import NullableList, Document


class NullableListTests(unittest.TestCase):
    def test_deleting_an_item_sets_its_index_to_None(self):
        l = NullableList((1, 1, 2, 3, 5))
        del l[1]
        del l['3']

        self.assertEqual(l, [1, None, 2, None, 5])


class DocumentTests(unittest.TestCase):
    def test_documents_list_class_is_a_nullable_list(self):
        doc = Document({'dog': 'Xablau', 'siblings': ['Xena']})

        self.assertEqual(doc.list_class, NullableList)
        self.assertIsInstance(doc['siblings'], NullableList)

    def test_documents_creates_path_by_default(self):
        doc = Document({})
        self.assertTrue(doc.create_if_not_exists)

        doc['y.m.c.a'] = "Village People"
        self.assertEqual(doc['y']['m']['c']['a'], "Village People")
