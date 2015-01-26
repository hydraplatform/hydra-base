<%inherit file="local:templates.master"/>

<%def name="title()">
Turbogears Administration System
</%def>

  <div class="row">
    <div class="col-md-12">
      <h1 class="page-header">TurboGears Admin</h1>
      <p class="lead">
        This is a fully-configurable administrative tool to help you administer your website.<br/>
        Below is links to all of your models.
        They will bring you to a listing of the objects in your database.
      </p>
    </div>
  </div>

  <br/>

  <div class="row">
    <div class="col-md-12">
      <div class="list-group">
        % for model in models:
        <a class="list-group-item" href='${model.lower()}s/'>
          <h4 class="list-group-item-heading">
            <span class="glyphicon glyphicon-list-alt"></span> ${model}s
          </h4>
        </a>
        % endfor
      </div>
    </div>
  </div>