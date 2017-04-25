.. _contributing:

Contributing
============

We value all kinds of contributions from the community, not just actual
code. Perhaps the easiest and yet one of the most valuable ways of
helping us improve GeoPySpark is to ask questions, voice concerns or
propose improvements on the GeoTrellis `Mailing
List <https://locationtech.org/mailman/listinfo/geotrellis-user>`__.
As of now, we will be using this to interact with our users. However, this
could change depending on the volume/interest of users.

If you do like to contribute actual code in the form of bug fixes, new
features or other patches this page gives you more info on how to do it.

Building GeoPySpark
-------------------

1. Install and setup Hadoop (the master branch is currently built with 2.0.1).
2. Check out this repository.
3. Pick the branch corresponding to the version you are targeting
4. Run ``make install`` to build GeoPyspark.

Style Guide
-----------

We try to follow the `PEP 8 Style Guide for Python Code
<https://www.python.org/dev/peps/pep-0008/>`_ as closely as possible,
although you will see some variations throughout the codebase. When in
doubt, follow that guide.

Git Branching Model
-------------------

The GeoPySpark team follows the standard practice of using the
``master`` branch as main integration branch.

Git Commit Messages
-------------------

We follow the 'imperative present tense' style for commit messages.
(e.g. "Add new EnterpriseWidgetLoader instance")

Issue Tracking
--------------

If you find a bug and would like to report it please go there and create
an issue. As always, if you need some help join us on
`Gitter <https://gitter.im/locationtech/geotrellis>`__ to chat with a
developer. As with the mailing list, we will be using the GeoTrellis
gitter channel until the need arises to form our own.

Pull Requests
-------------

If you'd like to submit a code contribution please fork GeoPySpark and
send us pull request against the ``master`` branch. Like any other open
source project, we might ask you to go through some iterations of
discussion and refinement before merging.

As part of the Eclipse IP Due Diligence process, you'll need to do some
extra work to contribute. This is part of the requirement for Eclipse
Foundation projects (`see this page in the Eclipse
wiki <https://wiki.eclipse.org/Development_Resources/Handling_Git_Contributions#Git>`__
You'll need to sign up for an Eclipse account **with the same email you
commit to github with**. See the ``Eclipse Contributor Agreement`` text
below. Also, you'll need to signoff on your commits, using the
``git commit -s`` flag. See
https://help.github.com/articles/signing-tags-using-gpg/ for more info.

Eclipse Contributor Agreement (ECA)
-----------------------------------

Contributions to the project, no matter what kind, are always very
welcome. Everyone who contributes code to GeoTrellis will be asked to
sign the Eclipse Contributor Agreement. You can electronically sign the
`Eclipse Contributor Agreement
here <https://www.eclipse.org/legal/ECA.php>`__.

Editing these Docs
------------------

Contributions to these docs are welcome as well. To build them on your own
machine, ensure that ``sphinx`` and ``make`` are installed.

Installing Dependencies
^^^^^^^^^^^^^^^^^^^^^^^

Ubuntu 16.04
''''''''''''

.. code:: console

   > sudo apt-get install python-sphinx python-sphinx-rtd-theme

Arch Linux
''''''''''

.. code:: console

   > sudo pacman -S python-sphinx python-sphinx_rtd_theme

MacOS
'''''

``brew`` doesn't supply the sphinx binaries, so use ``pip`` here.

Pip
'''

.. code:: console

   > pip install sphinx sphinx_rtd_theme

Building the Docs
^^^^^^^^^^^^^^^^^

Assuming you've cloned the `GeoTrellis repo
<https://github.com/locationtech/geotrellis>`__, you can now build the docs
yourself. Steps:

1. Navigate to the ``docs/`` directory
2. Run ``make html``
3. View the docs in your browser by opening ``_build/html/index.html``

.. note:: Changes you make will not be automatically applied; you will have
          to rebuild the docs yourself. Luckily the docs build in about a second.

File Structure
^^^^^^^^^^^^^^

There is currently not a file structure in place for docs. Though, this will
change soon.
